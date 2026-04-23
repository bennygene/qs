import subprocess
import argparse
import os
 
DASHBOARD_IDS_FILE = "dashboard_ids.txt"  # Path to your dashboard IDs file
DASHBOARD_SUCCESS_FILE = "dashboard_id_success.txt"
 
ACCOUNT_ID = "183631319430"
#region = "us-east-1", "eu-central-1","ap-south-1","ap-southeast-2","eu-west-1"
REGION = "eu-central-1"
FOLDER_PATH = "./quicksight/downloads"
EXPORT_SCRIPT = "exportDashboardNEW.py"
CLEAN_SCRIPT = "cleanZIP.py"
IMPORT_SCRIPT = "importDashboardNEW.py"
OVERRIDE_FILE = "cat_override_183631319430_eu-central-1_to_173294455309_eu-central-1.json"
IMPORT_ACCOUNT_ID = "173294455309"
IMPORT_REGION = "eu-central-1"
PROFILE = "target"
#target=dev,cat,prod
DEFAULT_ENV = "cat"
 
def get_success_dashboard_ids():
    if not os.path.exists(DASHBOARD_SUCCESS_FILE):
        return set()
    with open(DASHBOARD_SUCCESS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())
 
def add_success_dashboard_id(dashboard_id):
    with open(DASHBOARD_SUCCESS_FILE, "a", encoding="utf-8") as f:
        f.write(dashboard_id + "\n")
 
def export_dashboards():
    dashboard_ids = []
    with open(DASHBOARD_IDS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split('\t')
            # Skip header or lines that don't have at least 4 columns
            if len(parts) < 4 or parts[3] == "Dashboard ID":
                continue
            dashboard_ids.append(parts[3])
 
    for dashboard_id in dashboard_ids:
        cmd = [
            "python",
            EXPORT_SCRIPT,
            "--account-id", ACCOUNT_ID,
            "--region", REGION,
            "--dashboard-id", dashboard_id,
            "--folder-path", FOLDER_PATH
        ]
        print(f"Exporting dashboard: {dashboard_id}")
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"Error running command for dashboard {dashboard_id}: {e}")
 
def clean_zips(input_folder, env=DEFAULT_ENV):
    # Find all .zip files in the input folder
    zip_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.zip')]
    if not zip_files:
        print(f"No zip files found in {input_folder}")
        return
    for zip_file in zip_files:
        zip_path = os.path.join(input_folder, zip_file)
        cmd = [
            "python",
            CLEAN_SCRIPT,
            zip_path,
            "--env", env
        ]
        print(f"Cleaning zip file: {zip_path} with env: {env}")
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"Error running cleanZipNEW.py for {zip_file}: {e}")
 
def import_dashboards(input_folder, override_file=OVERRIDE_FILE, profile=PROFILE):
    success_ids = get_success_dashboard_ids()
    zip_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.zip')]
    if not zip_files:
        print(f"No zip files found in {input_folder}")
        return
    for zip_file in zip_files:
        base_name = os.path.splitext(os.path.basename(zip_file))[0]
        if base_name in success_ids:
            print(f"Skipping already imported dashboard: {base_name}")
            continue
 
        zip_path = os.path.join(input_folder, zip_file)
        dashboard_arn = base_name.upper().replace("-", "_").replace(" ", "_")
        job_id = base_name[:20].replace("-", "_").replace(" ", "_")  # limit job-id length if needed
 
        cmd = [
            "python",
            IMPORT_SCRIPT,
            "--account-id", IMPORT_ACCOUNT_ID,
            "--region", IMPORT_REGION,
            "--asset-bundle", zip_path,
            "--dashboard-arn", dashboard_arn,
            "--override", override_file,
            "--job-id", job_id,
            "--profile", profile
        ]
        print(f"Importing dashboard from: {zip_path} as ARN: {dashboard_arn}, job-id: {job_id}")
        try:
            subprocess.run(cmd, check=True)
            add_success_dashboard_id(base_name)
        except Exception as e:
            print(f"Error running importDashboard.py for {zip_file}: {e}")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run dashboard export, clean, or import process.")
    parser.add_argument(
        "--process",
        choices=["export", "clean", "import"],
        required=True,
        help="Specify which process to run: export, clean, or import"
    )
    parser.add_argument(
        "--env",
        default=DEFAULT_ENV,
        help="Environment to use for clean process (default: dev)"
    )
    parser.add_argument(
        "--input-folder",
        default="./quicksight/downloads",
        help="Input folder path for zip files (used for clean/import process)"
    )
    parser.add_argument(
        "--override-file",
        default=OVERRIDE_FILE,
        help="Override JSON file for import process"
    )
    parser.add_argument(
        "--profile",
        default=PROFILE,
        help="AWS CLI profile for import process"
    )
    args = parser.parse_args()
 
    if args.process == "export":
        export_dashboards()
    elif args.process == "clean":
        clean_zips(args.input_folder, env=args.env)
    elif args.process == "import":
        import_dashboards(args.input_folder, override_file=args.override_file, profile=args.profile)
