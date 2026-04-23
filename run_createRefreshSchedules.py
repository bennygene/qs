import argparse
from datetime import datetime, timezone
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
        return json.loads(process.stdout) if process.stdout.strip() else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse JSON output: {exc}") from exc


def build_args():
    parser = argparse.ArgumentParser(
        description="Create QuickSight dataset refresh schedules from a saved list JSON file."
    )
    parser.add_argument(
        "input_file",
        help="Path to datasets_with_scheduled_refresh_<account>_<region>.json",
    )
    parser.add_argument(
        "--account-id",
        help="AWS account ID (overrides value from input file).",
    )
    parser.add_argument(
        "--region",
        help="AWS region (overrides value from input file).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print create commands but do not execute them.",
    )
    parser.add_argument(
        "--skip-errors",
        action="store_true",
        help="Continue creating schedules if one fails.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip schedules that already exist (detected by error text).",
    )
    return parser.parse_args()


def make_create_command(account_id, region, dataset_id, schedule, no_verify_ssl):
    cmd = [
        "aws",
        "quicksight",
        "create-refresh-schedule",
        "--aws-account-id",
        account_id,
        "--data-set-id",
        dataset_id,
        "--region",
        region,
        "--schedule",
        json.dumps(schedule, separators=(",", ":")),
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    return cmd


def is_already_exists_error(message):
    text = message.lower()
    return "already exists" in text or "resourceexistsexception" in text or "conflict" in text


def normalize_schedule_for_create(schedule):
    """
    Keep only fields accepted by create-refresh-schedule and drop stale StartAfterDateTime.
    """
    cleaned = {
        "ScheduleId": schedule.get("ScheduleId"),
        "RefreshType": schedule.get("RefreshType"),
        "ScheduleFrequency": schedule.get("ScheduleFrequency"),
    }

    start_after = schedule.get("StartAfterDateTime")
    if start_after:
        try:
            # AWS list output includes ISO timestamps, often with timezone offsets.
            start_dt = datetime.fromisoformat(start_after)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            if start_dt > datetime.now(timezone.utc):
                cleaned["StartAfterDateTime"] = start_after
        except ValueError:
            # If parsing fails, omit this optional field so create call can proceed.
            pass

    return cleaned


def main():
    args = build_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}")
        sys.exit(1)

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Failed to parse JSON from {input_path}: {exc}")
        sys.exit(1)

    account_id = args.account_id or payload.get("AwsAccountId")
    region = args.region or payload.get("Region")
    no_verify_ssl = bool(payload.get("NoVerifySsl", True))
    items = payload.get("Items", [])

    if not account_id or not region:
        print("[ERROR] AWS account ID and region must be provided via --account-id/--region or in input file.")
        sys.exit(1)

    if not items:
        print("[WARN] No datasets found in input file.")
        sys.exit(0)

    total_to_create = 0
    for item in items:
        total_to_create += len(item.get("RefreshSchedules", []))

    print(f"[INFO] Loaded {len(items)} dataset(s), {total_to_create} schedule(s) to create.")
    if args.dry_run:
        print("[INFO] DRY-RUN mode: Commands will not be executed.")

    created_count = 0
    skipped_existing_count = 0
    failed_count = 0
    processed_count = 0

    for item in items:
        dataset_id = item.get("DataSetId")
        dataset_name = item.get("Name")
        schedules = item.get("RefreshSchedules", [])

        if not dataset_id or not schedules:
            continue

        for schedule in schedules:
            processed_count += 1
            schedule_id = schedule.get("ScheduleId")
            schedule_for_create = normalize_schedule_for_create(schedule)
            cmd = make_create_command(
                account_id=account_id,
                region=region,
                dataset_id=dataset_id,
                schedule=schedule_for_create,
                no_verify_ssl=no_verify_ssl,
            )

            if args.dry_run:
                print(
                    f"[DRY-RUN] [{processed_count}/{total_to_create}] "
                    f"{dataset_name} ({dataset_id}) schedule={schedule_id}"
                )
                print(f"  {' '.join(cmd)}")
                created_count += 1
                continue

            print(
                f"[{processed_count}/{total_to_create}] Creating schedule {schedule_id} "
                f"for {dataset_name} ({dataset_id})...",
                end=" ",
            )

            try:
                run_json_command(cmd)
                print("[OK]")
                created_count += 1
            except RuntimeError as exc:
                error_msg = str(exc)
                if args.skip_existing and is_already_exists_error(error_msg):
                    print("[SKIPPED - already exists]")
                    skipped_existing_count += 1
                    continue

                print(f"[FAILED] {error_msg}")
                failed_count += 1

                if not args.skip_errors:
                    print("[ERROR] Stopping due to error. Use --skip-errors to continue.")
                    sys.exit(1)

    print(
        "\n[SUMMARY] "
        f"Processed: {processed_count}, "
        f"Created: {created_count}, "
        f"SkippedExisting: {skipped_existing_count}, "
        f"Failed: {failed_count}"
    )

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
