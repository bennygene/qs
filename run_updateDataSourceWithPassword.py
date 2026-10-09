import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def run_json_command(args_list):
    """
    Runs a command and returns parsed JSON output.
    Raises RuntimeError on non-zero exit or invalid JSON.
    """
    # Mask credential blobs in printed command output
    safe_args = []
    skip_next = False
    for arg in args_list:
        if skip_next:
            safe_args.append("****")
            skip_next = False
        elif arg in ("--credentials", "--data-source-credentials"):
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


def describe_data_source(account_id, region, data_source_id, no_verify_ssl=True):
    cmd = [
        "aws",
        "quicksight",
        "describe-data-source",
        "--aws-account-id",
        account_id,
        "--data-source-id",
        data_source_id,
        "--region",
        region,
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    payload = run_json_command(cmd)
    return payload.get("DataSource", {})


def get_and_validate_secret(secret_arn, region, no_verify_ssl, out_dir):
    cmd = [
        "aws",
        "secretsmanager",
        "get-secret-value",
        "--secret-id",
        secret_arn,
        "--region",
        region,
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    payload = run_json_command(cmd)
    secret_string = payload.get("SecretString")
    if not secret_string:
        raise RuntimeError("SecretString is empty. This script expects a JSON string secret.")

    try:
        secret_json = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"SecretString is not valid JSON: {exc}") from exc

    if not isinstance(secret_json, dict):
        raise RuntimeError("SecretString JSON must be an object with username/password fields.")

    username = secret_json.get("username")
    password = secret_json.get("password")
    if not username or not password:
        raise RuntimeError(
            "SecretString JSON must include non-empty 'username' and 'password' fields."
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    secret_dump_path = out_dir / f"validated_secret_value_{ts}.json"
    secret_dump_path.write_text(json.dumps(secret_json, indent=2), encoding="utf-8")
    return username, password, secret_dump_path

def filter_by_data_source_type(data_sources, data_source_types):
    if not data_source_types:
        return data_sources

    expected_types = {item.strip().upper() for item in data_source_types if item.strip()}
    return [
        source
        for source in data_sources
        if str(source.get("Type", "")).upper() in expected_types
    ]


def load_data_sources_from_file(file_path):
    path = Path(file_path)
    if not path.is_file():
        raise RuntimeError(f"DataSources file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse DataSources file JSON: {exc}") from exc

    items = payload.get("Items", [])
    if not isinstance(items, list):
        raise RuntimeError("Invalid DataSources file format: 'Items' must be a list")

    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        data_source_id = item.get("DataSourceId")
        if not data_source_id:
            continue
        normalized.append(
            {
                "DataSourceId": data_source_id,
                "Name": item.get("Name", ""),
                "Type": item.get("Type", ""),
                "Status": item.get("Status", ""),
            }
        )

    return payload, normalized


def load_secret_arn_from_file(file_path, account_id, region):
    path = Path(file_path)
    if not path.is_file():
        raise RuntimeError(f"Secret file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse secret file JSON: {exc}") from exc

    secret_arn = payload.get("secret_arn") or payload.get("secretArn") or payload.get("SecretArn")
    if secret_arn:
        return secret_arn

    secret_name = payload.get("secret_name") or payload.get("secretName") or payload.get("SecretName")
    if secret_name:
        return f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_name}"

    raise RuntimeError(
        "Secret file must include one of: secret_arn/secretArn/SecretArn "
        "or secret_name/secretName/SecretName"
    )


def update_data_source_with_password(
    account_id,
    region,
    data_source_id,
    data_source_name,
    username,
    password,
    data_source_parameters,
    no_verify_ssl=True,
    vpc_connection_properties=None,
    ssl_properties=None,
):
    credentials_payload = {
        "CredentialPair": {
            "Username": username,
            "Password": password,
        }
    }
    credentials = json.dumps(credentials_payload)

    cmd = [
        "aws",
        "quicksight",
        "update-data-source",
        "--aws-account-id",
        account_id,
        "--data-source-id",
        data_source_id,
        "--name",
        data_source_name,
        "--credentials",
        credentials,
        "--data-source-parameters",
        json.dumps(data_source_parameters),
        "--region",
        region,
        "--output",
        "json",
    ]

    if vpc_connection_properties:
        cmd.extend(["--vpc-connection-properties", json.dumps(vpc_connection_properties)])

    if ssl_properties:
        cmd.extend(["--ssl-properties", json.dumps(ssl_properties)])

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    return run_json_command(cmd)

def ensure_password_authentication_type(data_source_parameters):
    params = json.loads(json.dumps(data_source_parameters))
    changed = False

    if isinstance(params.get("SnowflakeParameters"), dict):
        if params["SnowflakeParameters"].get("AuthenticationType") != "PASSWORD":
            params["SnowflakeParameters"]["AuthenticationType"] = "PASSWORD"
            changed = True

    if isinstance(params.get("StarburstParameters"), dict):
        if params["StarburstParameters"].get("AuthenticationType") != "PASSWORD":
            params["StarburstParameters"]["AuthenticationType"] = "PASSWORD"
            changed = True

    return params, changed


def build_args():
    parser = argparse.ArgumentParser(
        description=(
            "Update QuickSight DataSource credentials using AWS Secrets Manager (Password auth). "
            "Lists DataSources in the account/region, optionally filters by type or ID, "
            "then updates each matched DataSource to use Password credentials loaded from the provided secret."
        )
    )
    parser.add_argument("--account-id", default="975049925760", help="AWS account ID that owns the QuickSight assets.")
    parser.add_argument("--region", default="us-east-1", help="AWS region for QuickSight API calls.")
    parser.add_argument("--secret-arn", default=None, help="AWS Secrets Manager ARN containing username and password for database authentication")
    parser.add_argument(
        "--secret-file",
        default="datasource_secret_arn_password.json",
        help=(
            "JSON file containing secret_arn or secret_name to resolve credentials from AWS Secrets Manager.\n            Default: datasource_secret_arn_password.json"
        ),
    )
    parser.add_argument(
        "--skip-secret-precheck",
        action="store_true",
        help="Skip Secrets Manager get-secret-value validation step.",
    )
    parser.add_argument(
        "--data-sources-file",
        default=None,
        help=(
            "Optional JSON file generated by run_listDataSource.py. "
            "When provided, updates are based on the file's Items list instead of listing from AWS."
        ),
    )
    parser.add_argument(
        "--data-source-type",
        action="append",
        default=[],
        help=(
            "Optional DataSource type filter (for example REDSHIFT, AURORA_POSTGRESQL, MYSQL). "
            "Repeat the flag to include multiple types. Omit to target all DataSources."
        ),
    )
    parser.add_argument(
        "--data-source-id",
        action="append",
        default=[],
        help="Optional DataSource ID to target. Repeat the flag for multiple IDs.",
    )
    parser.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "downloads"), help="Directory where the results JSON file will be written.")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL certificate verification for AWS CLI calls.")
    parser.add_argument("--verify-ssl", action="store_true", help="Enable SSL certificate verification for AWS CLI calls.")
    parser.add_argument("--dry-run", action="store_true", help="List matched DataSources without applying updates.")
    return parser.parse_args()


def main():
    args = build_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    src_account_id = args.account_id
    src_region = args.region
    no_verify_ssl = True

    if args.verify_ssl:
        no_verify_ssl = False
    elif args.no_verify_ssl:
        no_verify_ssl = True

    secret_arn = args.secret_arn
    if args.secret_file:
        secret_arn = load_secret_arn_from_file(args.secret_file, src_account_id, src_region)
        print(f"Loaded Secret ARN from file: {args.secret_file}")

    if not secret_arn:
        print("[ERROR] Provide --secret-arn or --secret-file")
        sys.exit(1)

    if args.skip_secret_precheck:
        print("[WARN] --skip-secret-precheck is ignored in Password mode. Secret is required to build CredentialPair.")

    username, password, secret_dump_path = get_and_validate_secret(secret_arn, src_region, no_verify_ssl, out_dir)
    print(f"[OK] Secret loaded for Password auth and saved to: {secret_dump_path}")

    selected_types = [item.strip().upper() for item in args.data_source_type if item.strip()]
    selected_ids = {item.strip() for item in args.data_source_id if item.strip()}

    source_file_used = None
    if args.data_sources_file:
        print(f"Loading data sources from file: {args.data_sources_file}")
        payload, data_sources = load_data_sources_from_file(args.data_sources_file)
        source_file_used = str(Path(args.data_sources_file).resolve())
        print(f"Loaded {len(data_sources)} data source(s) from file.")

        file_account = payload.get("AwsAccountId")
        file_region = payload.get("Region")
        if file_account and str(file_account) != str(src_account_id):
            print(f"[WARN] account-id differs from file metadata: cli={src_account_id}, file={file_account}. Using CLI account-id.")
        if file_region and str(file_region) != str(src_region):
            print(f"[WARN] region differs from file metadata: cli={src_region}, file={file_region}. Using CLI region.")
    else:
        print(f"Listing data sources for account={src_account_id}, region={src_region}")
        data_sources = list_all_data_sources(src_account_id, src_region, no_verify_ssl=no_verify_ssl)
        print(f"Found {len(data_sources)} total data source(s).")

    if selected_types:
        data_sources = filter_by_data_source_type(data_sources, selected_types)
        print(f"Applied type filter: {', '.join(selected_types)} | Matched {len(data_sources)} data source(s).")

    if selected_ids:
        data_sources = [s for s in data_sources if s.get("DataSourceId", "") in selected_ids]
        print(f"Applied ID filter | Matched {len(data_sources)} data source(s).")

    if not data_sources:
        print("No DataSources matched. Nothing to update.")
        return

    if args.dry_run:
        print("[DRY RUN] The following DataSources would be updated with Password credentials from secret:")
        for source in data_sources:
            print(f"  {source.get('Name')} ({source.get('DataSourceId')}) | Type={source.get('Type')} | Status={source.get('Status')}")
        print(f"\nSecret ARN: {secret_arn}")
        return

    results = []
    print(f"\nUpdating {len(data_sources)} data source(s) with Password credentials from secret: {secret_arn}\n")

    for source in data_sources:
        data_source_id = source.get("DataSourceId", "")
        listed_name = source.get("Name", "")
        source_type = source.get("Type", "")

        try:
            detail = describe_data_source(src_account_id, src_region, data_source_id, no_verify_ssl=no_verify_ssl)
            current_name = detail.get("Name") or listed_name or data_source_id
            params = detail.get("DataSourceParameters")
            vpc_props = detail.get("VpcConnectionProperties")
            ssl_props = detail.get("SslProperties")

            if not params:
                raise RuntimeError("DescribeDataSource did not return DataSourceParameters")

            params, auth_type_changed = ensure_password_authentication_type(params)
            if auth_type_changed:
                print(f"[INFO] Set DataSourceParameters.AuthenticationType=PASSWORD for: {current_name} ({data_source_id})")

            update_data_source_with_password(
                src_account_id,
                src_region,
                data_source_id,
                current_name,
                username,
                password,
                params,
                no_verify_ssl=no_verify_ssl,
                vpc_connection_properties=vpc_props,
                ssl_properties=ssl_props,
            )
            status = "SUCCESS"
            error = None
            print(f"[OK] Updated credentials for: {current_name} ({data_source_id})")
        except RuntimeError as exc:
            status = "FAILED"
            error = str(exc)
            print(f"[FAIL] {listed_name} ({data_source_id}): {error}")

        results.append(
            {
                "DataSourceId": data_source_id,
                "Name": listed_name,
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
        "AuthType": "Password",
        "SecretArn": secret_arn,
        "SecretFile": str(Path(args.secret_file).resolve()) if args.secret_file else None,
        "ValidatedSecretFile": str(secret_dump_path) if secret_dump_path else None,
        "DataSourcesFile": source_file_used,
        "DataSourceTypeFilter": selected_types,
        "TotalMatched": len(results),
        "SuccessCount": success_count,
        "FailCount": fail_count,
        "Items": results,
    }

    out_file = out_dir / f"update_datasource_secret_arn_{src_account_id}_{src_region}.json"
    out_file.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print(f"\n[OK] Saved results to: {out_file}")
    print(f"Updated: {success_count} | Failed: {fail_count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)













