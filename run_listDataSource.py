import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_json_command(args_list):
    """
    Runs a command and returns parsed JSON output.
    Raises RuntimeError on non-zero exit or invalid JSON.
    """
    print("Running:", " ".join(f'"{a}"' if " " in a else a for a in args_list))

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


def build_args():
    parser = argparse.ArgumentParser(
        description="List QuickSight DataSources available in an account and region."
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
        "--out-dir",
        default=str(Path(__file__).resolve().parent / "downloads"),
        help="Directory where the output JSON file will be written.",
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
        "--data-source-type",
        action="append",
        default=[],
        help=(
            "Optional DataSource type filter (for example ATHENA, REDSHIFT, S3). "
            "Repeat the flag to include multiple types."
        ),
    )
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

    selected_types = [item.strip().upper() for item in args.data_source_type if item.strip()]

    print(f"Listing data sources for account={src_account_id}, region={src_region}")
    data_sources = list_all_data_sources(
        src_account_id,
        src_region,
        no_verify_ssl=no_verify_ssl,
    )
    print(f"Found {len(data_sources)} total data source(s).")

    if selected_types:
        data_sources = filter_by_data_source_type(data_sources, selected_types)
        print(
            "Applied type filter: "
            f"{', '.join(selected_types)} | Matched {len(data_sources)} data source(s)."
        )

    items = []

    for source in data_sources:
        data_source_id = source.get("DataSourceId", "")
        name = source.get("Name", "")
        source_type = source.get("Type", "")
        status = source.get("Status", "")
        created_time = source.get("CreatedTime")
        last_updated_time = source.get("LastUpdatedTime")

        items.append(
            {
                "DataSourceId": data_source_id,
                "Name": name,
                "Type": source_type,
                "Status": status,
                "CreatedTime": created_time,
                "LastUpdatedTime": last_updated_time,
                "Arn": source.get("Arn", ""),
            }
        )

        print(
            f"[OK] {name} ({data_source_id}) | Type={source_type} | Status={status}"
        )

    output_payload = {
        "AwsAccountId": src_account_id,
        "Region": src_region,
        "DataSourceTypeFilter": selected_types,
        "TotalDataSources": len(items),
        "Items": items,
    }

    out_file = out_dir / f"datasources_{src_account_id}_{src_region}.json"
    out_file.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print(f"[OK] Saved JSON to: {out_file}")
    print(f"Total data sources: {len(items)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
