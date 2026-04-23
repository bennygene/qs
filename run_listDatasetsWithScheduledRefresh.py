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


def list_all_datasets(account_id, region, no_verify_ssl=True):
    datasets = []
    next_token = None

    while True:
        cmd = [
            "aws",
            "quicksight",
            "list-data-sets",
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
        datasets.extend(payload.get("DataSetSummaries", []))
        next_token = payload.get("NextToken")

        if not next_token:
            break

    return datasets


def list_refresh_schedules(account_id, region, dataset_id, no_verify_ssl=True):
    cmd = [
        "aws",
        "quicksight",
        "list-refresh-schedules",
        "--aws-account-id",
        account_id,
        "--data-set-id",
        dataset_id,
        "--region",
        region,
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    payload = run_json_command(cmd)
    return payload.get("RefreshSchedules", [])


def build_args():
    parser = argparse.ArgumentParser(
        description="List QuickSight datasets that have scheduled refresh configured."
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

    print(f"Listing datasets for account={src_account_id}, region={src_region}")
    datasets = list_all_datasets(
        src_account_id,
        src_region,
        no_verify_ssl=no_verify_ssl,
    )
    print(f"Found {len(datasets)} total datasets.")

    datasets_with_schedule = []

    for item in datasets:
        dataset_id = item.get("DataSetId")
        dataset_name = item.get("Name")

        if not dataset_id:
            continue

        try:
            schedules = list_refresh_schedules(
                src_account_id,
                src_region,
                dataset_id,
                no_verify_ssl=no_verify_ssl,
            )
        except RuntimeError as exc:
            print(f"[WARN] Could not list schedules for {dataset_name} ({dataset_id}): {exc}")
            continue

        if schedules:
            datasets_with_schedule.append(
                {
                    "DataSetId": dataset_id,
                    "Name": dataset_name,
                    "ImportMode": item.get("ImportMode"),
                    "RefreshScheduleCount": len(schedules),
                    "RefreshSchedules": schedules,
                }
            )
            print(f"[OK] {dataset_name} ({dataset_id}) has {len(schedules)} schedule(s).")

    output_payload = {
        "AwsAccountId": src_account_id,
        "Region": src_region,
        "TotalDatasets": len(datasets),
        "DatasetsWithScheduledRefresh": len(datasets_with_schedule),
        "Items": datasets_with_schedule,
    }

    out_file = out_dir / f"datasets_with_scheduled_refresh_{src_account_id}_{src_region}.json"
    out_file.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print(f"[OK] Saved JSON to: {out_file}")
    print(f"Datasets with schedule: {len(datasets_with_schedule)}")

    # Build removal input file
    removal_items = []
    for ds in datasets_with_schedule:
        for sched in ds.get("RefreshSchedules", []):
            schedule_id = sched.get("ScheduleId")
            delete_cmd = (
                f"aws quicksight delete-refresh-schedule"
                f" --aws-account-id {src_account_id}"
                f" --data-set-id {ds['DataSetId']}"
                f" --schedule-id {schedule_id}"
                f" --region {src_region}"
            )
            if no_verify_ssl:
                delete_cmd += " --no-verify-ssl"
            removal_items.append(
                {
                    "DataSetId": ds["DataSetId"],
                    "DataSetName": ds["Name"],
                    "ScheduleId": schedule_id,
                    "DeleteCommand": delete_cmd,
                }
            )

    removal_payload = {"Items": removal_items}
    removal_file = out_dir / f"datasets_refresh_schedule_remove_input_{src_account_id}_{src_region}.json"
    removal_file.write_text(json.dumps(removal_payload, indent=2), encoding="utf-8")
    print(f"[OK] Saved removal input JSON to: {removal_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
