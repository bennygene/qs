import subprocess
import re
from pathlib import Path


def run_command_and_wait(cmd, success_regex):
    cmd = [str(x) for x in cmd]

    print("Running:", " ".join(f'"{a}"' if " " in a else a for a in cmd))

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    success_found = False
    error_found = False

    while True:
        line = p.stdout.readline()
        if line == "" and p.poll() is not None:
            break
        if line:
            line = line.rstrip()
            print(line)

            if "An error occurred" in line:
                error_found = True

            if re.search(success_regex, line):
                success_found = True

    p.wait()
    return success_found and not error_found and p.returncode == 0


def main():
    out_dir = (
        Path.home()
        / "OneDrive - Fiserv Corp"
        / "Documents"
        / "Working Folder"
        / "AWS Automation"
        / "quicksight"
        / "quicksight"
        / "downloads"
    )

    print("Path =", out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    aws_account_id = "975049925760"
    region = "us-east-1"
    dataset_id = "1a45bb02-2cd0-4f55-b4d3-9316b6c05713"

    # ✅ Windows-safe for AWS CLI
    json_file = f"file://{(out_dir / f'dataset_{dataset_id}_updated.json').as_posix()}"

    print("CLI JSON =", json_file)

    cmd = [
        "aws", "quicksight", "update-data-set",
        "--aws-account-id", aws_account_id,
        "--region", region,
        "--data-set-id", dataset_id,
        "--cli-input-json", json_file,
        "--no-cli-pager",
        "--no-verify-ssl"
    ]

    success = run_command_and_wait(cmd, success_regex=r'"Arn"\s*:\s*"')
    if not success:
        print("❌ Update did not appear to succeed.")
        return

    print("✅ Update wrapper completed successfully.")


if __name__ == "__main__":
    main()
