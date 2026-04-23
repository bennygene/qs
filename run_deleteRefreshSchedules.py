import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_command(args_list):
    """
    Runs a command and returns the result (return code and output).
    """
    print("Running:", " ".join(f'"{a}"' if " " in a else a for a in args_list))

    process = subprocess.run(
        args_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    return process.returncode, process.stdout, process.stderr


def build_args():
    parser = argparse.ArgumentParser(
        description="Delete QuickSight dataset refresh schedules from removal input file."
    )
    parser.add_argument(
        "input_file",
        help="Path to the removal input JSON file (datasets_refresh_schedule_remove_input_*.json).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands but do not execute them.",
    )
    parser.add_argument(
        "--skip-errors",
        action="store_true",
        help="Continue deleting other schedules if some deletions fail.",
    )
    return parser.parse_args()


def main():
    args = build_args()

    input_file = Path(args.input_file)
    if not input_file.exists():
        print(f"[ERROR] Input file not found: {input_file}")
        sys.exit(1)

    try:
        payload = json.loads(input_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Failed to parse JSON from {input_file}: {exc}")
        sys.exit(1)

    items = payload.get("Items", [])
    if not items:
        print("[WARN] No schedules found in input file.")
        sys.exit(0)

    print(f"[INFO] Loaded {len(items)} schedule(s) for deletion.")

    if args.dry_run:
        print("[INFO] DRY-RUN mode: Commands will NOT be executed.\n")

    deleted_count = 0
    failed_count = 0

    for idx, item in enumerate(items, start=1):
        dataset_id = item.get("DataSetId")
        dataset_name = item.get("DataSetName")
        schedule_id = item.get("ScheduleId")
        delete_command_str = item.get("DeleteCommand")

        if not delete_command_str:
            print(f"[WARN] [{idx}/{len(items)}] No delete command for {dataset_name} ({schedule_id})")
            continue

        if args.dry_run:
            print(f"[DRY-RUN] [{idx}/{len(items)}] {dataset_name} ({schedule_id})")
            print(f"  Command: {delete_command_str}\n")
            deleted_count += 1
        else:
            print(f"[{idx}/{len(items)}] Deleting schedule {schedule_id} from {dataset_name}...", end=" ")
            
            cmd_args = delete_command_str.split()
            returncode, stdout, stderr = run_command(cmd_args)

            if returncode == 0:
                print("[OK]")
                deleted_count += 1
            else:
                error_msg = stderr.strip() or stdout.strip()
                print(f"[FAILED] {error_msg}")
                failed_count += 1

                if not args.skip_errors:
                    print(f"[ERROR] Stopping due to error. Use --skip-errors to continue.")
                    sys.exit(1)

    print(f"\n[SUMMARY] Processed: {len(items)}, Deleted/Attempted: {deleted_count}, Failed: {failed_count}")
    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
