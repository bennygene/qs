import subprocess
import time
import re
from pathlib import Path

def run_command_and_wait(args_list):
    """
    Runs command as a list (no shell quoting issues), streams output,
    and detects success by matching the describe script's success line.
    """
    print("Running:", " ".join(f'"{a}"' if " " in a else a for a in args_list))

    process = subprocess.Popen(
        args_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    success_found = False

    while True:
        line = process.stdout.readline()
        if line == '' and process.poll() is not None:
            break
        if line:
            line = line.strip()
            print(line)

            # ✅ Match the actual success marker from describeDataSet.py
            if re.search(r'^\[OK\]\s+Saved JSON to:', line):
                success_found = True

    process.wait()
    return success_found and process.returncode == 0


def main():
    # ✅ Fix #1: always write to a folder you own
    out_dir = Path.home()/ "OneDrive - Fiserv Corp" / "Documents" / "Working Folder" / "AWS Automation" / "quicksight" / "quicksight" / "downloads"
    print("Path =", out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    src_account_id = "975049925760"
    src_region = "us-east-1"
    dataset_id = "1a45bb02-2cd0-4f55-b4d3-9316b6c05713"
    file_dname = f"dataset_{dataset_id}.json"
    out_file = out_dir / file_dname

    print("DEBUG out_file =", out_file)

    # ✅ Build command as a LIST (best practice on Windows)
    cmd = [
        "python", "describeDataSet.py",
        "--account-id", src_account_id,
        "--region", src_region,
        "--dataset-id", dataset_id,
        "--save-json", str(out_file)
    ]

    success = run_command_and_wait(cmd)
    if not success:
        print("Did not find '[OK] Saved JSON to:' message. Stopping batch.")
        return

    time.sleep(2)
    print("✅ Batch completed successfully.")

if __name__ == "__main__":
    main()
