import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def run_json_command(args_list):
    """
    Runs a command and returns parsed JSON output.
    Raises RuntimeError on non-zero exit or invalid JSON.
    """
    # Mask any credential values in the printed command
    safe_args = []
    skip_next = False
    for arg in args_list:
        if skip_next:
            safe_args.append("****")
            skip_next = False
        elif arg == "--data-source-credentials":
            safe_args.append(arg)
            skip_next = True
        else:
            safe_args.append(f'"{arg}"' if " " in arg else arg)
    print("Running:", " ".join(safe_args))

    process = subprocess.run(
        args_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if process.returncode != 0:
        raise RuntimeError(process.stderr.strip() or process.stdout.strip())

    try:
        return json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse JSON output: {exc}") from exc


def list_all_data_sources(account_id, region, no_verify_ssl=True):
    data_sources = []
    next_token = None

    while True:
        cmd = [
            "aws",
            "quicksight",
            "list-data-sources",
            "--aws-account-id",
            account_id,
            "--region",
            region,
            "--output",
            "json",
        ]

        if no_verify_ssl:
            cmd.append("--no-verify-ssl")

        if next_token:
            cmd.extend(["--next-token", next_token])

        payload = run_json_command(cmd)
        data_sources.extend(payload.get("DataSources", []))
        next_token = payload.get("NextToken")

        if not next_token:
            break

    return data_sources


def filter_by_data_source_type(data_sources, data_source_types):
    if not data_source_types:
        return data_sources

    expected_types = {item.strip().upper() for item in data_source_types if item.strip()}
    return [
        source
        for source in data_sources
        if str(source.get("Type", "")).upper() in expected_types
    ]


def update_data_source_credentials(account_id, region, data_source_id, username, password, no_verify_ssl=True):
    credentials = json.dumps(
        {"CredentialPair": {"Username": username, "Password": password}}
    )
    cmd = [
        "aws",
        "quicksight",
        "update-data-source-credentials",
        "--aws-account-id",
        account_id,
        "--data-source-id",
        data_source_id,
        "--data-source-credentials",
        credentials,
        "--region",
        region,
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    return run_json_command(cmd)


def build_args():
    parser = argparse.ArgumentParser(
        description=(
            "Update QuickSight DataSource credentials to a new username/password key pair. "
            "Lists all DataSources in the account/region, optionally filters by type, "
            "then calls update-data-source-credentials on each matched DataSource."
        )
    )
    parser.add_argument(
        "--account-id",
        default="975049925760",
        help="AWS account ID that owns the QuickSight assets.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for QuickSight API calls.",
    )
    parser.add_argument(
        "--credentials-file",
        default=None,
        help=(
            "Path to a JSON file containing 'username' and 'password' fields. "
            "Takes precedence over --username / --password and the "
            "QS_DATASOURCE_PASSWORD environment variable."
        ),
    )
    parser.add_argument(
        "--username",
        default=None,
        help="New username (key) to set on the DataSource credential pair.",
    )
    parser.add_argument(
        "--password",
        default=None,
        help=(
            "New password to set on the DataSource credential pair. "
            "If omitted, reads from QS_DATASOURCE_PASSWORD environment variable."
        ),
    )
    parser.add_argument(
        "--data-source-type",
        action="append",
        default=[],
        help=(
            "Optional DataSource type filter (for example REDSHIFT, AURORA_POSTGRESQL). "
            "Repeat the flag to include multiple types. Omit to target all DataSources."
        ),
    )
    parser.add_argument(
        "--data-source-id",
        action="append",
        default=[],
        help=(
            "Optional DataSource ID to target. "
            "Repeat the flag to target multiple specific DataSources. "
            "Omit to target all (or all matched by --data-source-type)."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parent / "downloads"),
        help="Directory where the results JSON file will be written.",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification for AWS CLI calls.",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        help="Enable SSL certificate verification for AWS CLI calls.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matched DataSources without applying any credential updates.",
    )
    return parser.parse_args()


def main():
    args = build_args()

    # Resolve credentials — credentials file takes top priority
    username = args.username
    password = os.environ.get("QS_DATASOURCE_PASSWORD") or args.password

    if args.credentials_file:
        creds_path = Path(args.credentials_file)
        if not creds_path.is_file():
            print(f"[ERROR] Credentials file not found: {creds_path}")
            sys.exit(1)
        try:
            creds = json.loads(creds_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"[ERROR] Failed to parse credentials file: {exc}")
            sys.exit(1)
        username = creds.get("username") or username
        password = creds.get("password") or password
        print(f"[INFO] Loaded credentials from: {creds_path}")

    if not username:
        print(
            "[ERROR] Username is required. Provide --username or include "
            "'username' in your --credentials-file."
        )
        sys.exit(1)
    if not password:
        print(
            "[ERROR] Password is required. Provide --password, set the "
            "QS_DATASOURCE_PASSWORD environment variable, or include "
            "'password' in your --credentials-file."
        )
        sys.exit(1)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    src_account_id = args.account_id
    src_region = args.region
    no_verify_ssl = True

    if args.verify_ssl:
        no_verify_ssl = False
    elif args.no_verify_ssl:
        no_verify_ssl = True

    selected_types = [item.strip().upper() for item in args.data_source_type if item.strip()]
    selected_ids = {item.strip() for item in args.data_source_id if item.strip()}

    print(f"Listing data sources for account={src_account_id}, region={src_region}")
    data_sources = list_all_data_sources(src_account_id, src_region, no_verify_ssl=no_verify_ssl)
    print(f"Found {len(data_sources)} total data source(s).")

    if selected_types:
        data_sources = filter_by_data_source_type(data_sources, selected_types)
        print(
            f"Applied type filter: {', '.join(selected_types)} "
            f"| Matched {len(data_sources)} data source(s)."
        )

    if selected_ids:
        data_sources = [s for s in data_sources if s.get("DataSourceId", "") in selected_ids]
        print(f"Applied ID filter | Matched {len(data_sources)} data source(s).")

    if not data_sources:
        print("No DataSources matched. Nothing to update.")
        return

    if args.dry_run:
        print("[DRY RUN] The following DataSources would be updated:")
        for source in data_sources:
            print(
                f"  {source.get('Name')} ({source.get('DataSourceId')}) "
                f"| Type={source.get('Type')} | Status={source.get('Status')}"
            )
        return

    results = []

    for source in data_sources:
        data_source_id = source.get("DataSourceId", "")
        name = source.get("Name", "")
        source_type = source.get("Type", "")

        try:
            update_data_source_credentials(
                src_account_id,
                src_region,
                data_source_id,
                username,
                password,
                no_verify_ssl=no_verify_ssl,
            )
            status = "SUCCESS"
            error = None
            print(f"[OK] Updated credentials for: {name} ({data_source_id})")
        except RuntimeError as exc:
            status = "FAILED"
            error = str(exc)
            print(f"[FAIL] {name} ({data_source_id}): {error}")

        results.append(
            {
                "DataSourceId": data_source_id,
                "Name": name,
                "Type": source_type,
                "UpdateStatus": status,
                "Error": error,
            }
        )

    success_count = sum(1 for r in results if r["UpdateStatus"] == "SUCCESS")
    fail_count = len(results) - success_count

    output_payload = {
        "AwsAccountId": src_account_id,
        "Region": src_region,
        "Username": username,
        "DataSourceTypeFilter": selected_types,
        "TotalMatched": len(results),
        "SuccessCount": success_count,
        "FailCount": fail_count,
        "Items": results,
    }

    out_file = out_dir / f"update_datasource_credentials_{src_account_id}_{src_region}.json"
    out_file.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print(f"\n[OK] Saved results to: {out_file}")
    print(f"Updated: {success_count} | Failed: {fail_count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
