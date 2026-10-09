import subprocess
import argparse
import os
import sys
import json
import re
import zipfile
from pathlib import Path

DASHBOARD_IDS_FILE = "dashboard_ids.txt"  # Path to your dashboard IDs file
DASHBOARD_SUCCESS_FILE = "dashboard_id_success.txt"
DATASOURCE_IDS_FILE = "datasource_ids_migrated.txt"

ACCOUNT_ID = "032559375825"
#region = "us-east-1", "eu-central-1","ap-south-1","ap-southeast-2","eu-west-1"
REGION = "eu-central-1"
FOLDER_PATH = "./quicksight/downloads"
EXPORT_SCRIPT = "exportDashboardNEW.py"
CLEAN_SCRIPT = "cleanZIP.py"
IMPORT_SCRIPT = "importDashboardNEW.py"
UPDATE_KEYPAIR_SCRIPT = "run_updateDataSourceWithKeyPair.py"
OVERRIDE_FILE = "cat_override_032559375825_eu-central-1_to_904233127335_us-east-1.json"
IMPORT_ACCOUNT_ID = "904233127335"
IMPORT_REGION = "us-east-1"
PROFILE = "target"
#target=dev,cat,prod
DEFAULT_ENV = "cat"
 
def verify_aws_auth(account_id, profile=None):
    """Verify AWS credentials are valid for the given account before proceeding."""
    cmd = ["aws", "sts", "get-caller-identity", "--output", "json", "--no-verify-ssl"]
    if profile:
        cmd.extend(["--profile", profile])
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        if result.returncode != 0:
            print(f"ERROR: AWS authentication failed for profile '{profile}':")
            print(result.stderr.strip())
            sys.exit(1)
        identity = json.loads(result.stdout)
        caller_account = identity.get("Account", "")
        if caller_account != account_id:
            print(f"ERROR: Authenticated account ({caller_account}) does not match expected account ({account_id}).")
            sys.exit(1)
        print(f"AWS auth verified: Account={caller_account}, ARN={identity.get('Arn', '')}")
    except FileNotFoundError:
        print("ERROR: AWS CLI not found. Ensure it is installed and on PATH.")
        sys.exit(1)


def get_success_dashboard_ids():
    if not os.path.exists(DASHBOARD_SUCCESS_FILE):
        return set()
    with open(DASHBOARD_SUCCESS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())
 
def add_success_dashboard_id(dashboard_id):
    with open(DASHBOARD_SUCCESS_FILE, "a", encoding="utf-8") as f:
        f.write(dashboard_id + "\n")


def derive_target_vpc_arn_from_override(override_file):
    """
    Prefer the first VPCConnectionID from override VPCConnections and build target ARN.
    Falls back to first VpcConnectionArn under DataSources when needed.
    """
    if not override_file:
        return None

    override_path = Path(override_file)
    if not override_path.is_absolute():
        override_path = Path(__file__).resolve().parent / override_path

    if not override_path.exists():
        print(f"[WARN] Override file not found for VPC ARN discovery: {override_path}")
        return None

    try:
        payload = json.loads(override_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[WARN] Failed to parse override file for VPC ARN discovery: {exc}")
        return None

    vpc_connections = payload.get("VPCConnections", [])
    if isinstance(vpc_connections, list) and vpc_connections:
        first_vpc = vpc_connections[0] if isinstance(vpc_connections[0], dict) else None
        if first_vpc:
            vpc_id = (
                first_vpc.get("VPCConnectionId")
                or first_vpc.get("VpcConnectionId")
                or first_vpc.get("vpcConnectionId")
            )
            if isinstance(vpc_id, str) and vpc_id.strip():
                return f"arn:aws:quicksight:{IMPORT_REGION}:{IMPORT_ACCOUNT_ID}:vpcConnection/{vpc_id.strip()}"

    data_sources = payload.get("DataSources", [])
    if isinstance(data_sources, list):
        for ds in data_sources:
            if not isinstance(ds, dict):
                continue
            vpc_props = ds.get("VpcConnectionProperties") or {}
            arn = vpc_props.get("VpcConnectionArn")
            if isinstance(arn, str) and arn.strip():
                return arn.strip()

    return None

def extract_datasource_ids_from_override(override_file):
    """Extract Snowflake datasource IDs and info from asset bundle ZIP files."""
    downloads_folder = "quicksight/downloads"
    if not os.path.exists(downloads_folder):
        print(f"Downloads folder not found: {downloads_folder}")
        return []
    
    datasources = []
    zip_files = [f for f in os.listdir(downloads_folder) if f.lower().endswith('.zip')]
    
    if not zip_files:
        print(f"No zip files found in {downloads_folder}")
        return []
    
    print(f"Found {len(zip_files)} zip files in {downloads_folder}")
    
    for zip_file in zip_files:
        zip_path = os.path.join(downloads_folder, zip_file)
        print(f"\nScanning zip file: {zip_path}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                print(f"  Files in zip: {len(file_list)} total files")
                
                # Look for datasource files (they're typically in datasource/ directory)
                datasource_files = [f for f in file_list if f.startswith('datasource/') and f.endswith('.json')]
                print(f"  Found {len(datasource_files)} datasource JSON files")
                
                for ds_file in datasource_files:
                    print(f"  Reading: {ds_file}")
                    try:
                        with zip_ref.open(ds_file) as json_file:
                            ds_data = json.load(json_file)
                            
                            # Extract datasource info from top-level keys
                            ds_type = ds_data.get("type", "")
                            ds_id = ds_data.get("dataSourceId", "")
                            ds_name = ds_data.get("name", "")
                            
                            print(f"    Type: {ds_type}, ID: {ds_id}, Name: {ds_name}")
                            
                            # Only extract Snowflake datasources
                            if ds_type.upper() == "SNOWFLAKE" or "snowflake" in ds_type.lower():
                                ds_info = {
                                    "DataSourceId": ds_id,
                                    "Name": ds_name,
                                    "Type": ds_type,
                                    "DataSourceParameters": ds_data.get("dataSourceParameters", {}),
                                    "VpcConnectionProperties": ds_data.get("vpcConnectionProperties", {})
                                }
                                datasources.append(ds_info)
                                print(f"    ✓ Added Snowflake datasource: {ds_id}")
                    except Exception as e:
                        print(f"    Error reading {ds_file}: {e}")
        except Exception as e:
            print(f"Error reading zip file {zip_file}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\nTotal Snowflake datasources extracted: {len(datasources)}")
    return datasources


def save_datasource_ids(datasources, output_file=DATASOURCE_IDS_FILE):
    """Save datasource information to a JSON file."""
    output_json_file = output_file.replace('.txt', '.json')
    
    datasource_data = {
        "datasources": datasources,
        "count": len(datasources),
        "timestamp": str(__import__('datetime').datetime.now())
    }
    
    with open(output_json_file, "w", encoding="utf-8") as f:
        json.dump(datasource_data, f, indent=2)
    
    print(f"Saved {len(datasources)} datasource entries to {output_json_file}")
    
    # Also save just the IDs to text file for convenience
    with open(output_file, "w", encoding="utf-8") as f:
        for ds in datasources:
            f.write(ds.get("DataSourceId", "") + "\n")
    
    print(f"Saved {len(datasources)} datasource IDs to {output_file}")

def convert_datasources_to_keypair(datasource_ids_file=DATASOURCE_IDS_FILE, secret_file="datasource_secret_arn_keypair.json", env=DEFAULT_ENV):
    """Convert Snowflake datasources to KEYPAIR authentication."""
    if not os.path.exists(datasource_ids_file):
        print(f"Datasource IDs file not found: {datasource_ids_file}")
        print("Run 'python runjobUsingKeypair.py --process extract-datasources' first")
        return
    
    with open(datasource_ids_file, "r", encoding="utf-8") as f:
        datasource_ids = [line.strip() for line in f if line.strip()]
    
    if not datasource_ids:
        print(f"No datasource IDs found in {datasource_ids_file}")
        return
    
    print(f"Converting {len(datasource_ids)} datasources to KEYPAIR authentication...")
    
    for ds_id in datasource_ids:
        cmd = [
            "python",
            UPDATE_KEYPAIR_SCRIPT,
            "--account-id", IMPORT_ACCOUNT_ID,
            "--region", IMPORT_REGION,
            "--secret-file", secret_file,
            "--data-source-id", ds_id,
            "--env", env,
        ]
        print(f"Converting datasource: {ds_id}")
        try:
            subprocess.run(cmd, check=True)
            print(f"Successfully converted datasource: {ds_id}")
        except Exception as e:
            print(f"Error converting datasource {ds_id}: {e}")

 
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
 
def clean_zips(input_folder, env=DEFAULT_ENV, target_vpc_arn=None, override_file=OVERRIDE_FILE):
    effective_target_vpc_arn = target_vpc_arn
    if not effective_target_vpc_arn:
        effective_target_vpc_arn = derive_target_vpc_arn_from_override(override_file)
        if effective_target_vpc_arn:
            print(f"Using target VPC ARN derived from override file: {effective_target_vpc_arn}")
        else:
            print("[WARN] No target VPC ARN provided and none found in override file. VPC ARN values in datasource JSON will not be rewritten.")

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
        if effective_target_vpc_arn:
            cmd.extend(["--target-vpc-arn", effective_target_vpc_arn])
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
        # AWS requires assetBundleImportJobId to match [\w\-]+
        job_id = re.sub(r"[^\w\-]", "_", base_name[:20])

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
    
    # Extract datasource IDs after all imports complete (from asset bundle ZIP files in downloads folder)
    print("\nExtracting datasource IDs from asset bundle files in downloads folder...")
    datasource_ids = extract_datasource_ids_from_override(None)
    save_datasource_ids(datasource_ids, DATASOURCE_IDS_FILE)
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run dashboard export, clean, import, or datasource conversion process.")
    parser.add_argument(
        "--process",
        choices=["export", "clean", "import", "extract-datasources", "convert-to-keypair"],
        required=True,
        help="Specify which process to run: export, clean, import, extract-datasources, or convert-to-keypair"
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
        "--target-vpc-arn",
        default=None,
        help="Optional target QuickSight VPC connection ARN for clean process."
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
    parser.add_argument(
        "--datasource-ids-file",
        default=DATASOURCE_IDS_FILE,
        help="File containing datasource IDs to convert (used with convert-to-keypair)"
    )
    parser.add_argument(
        "--secret-file",
        default="datasource_secret_arn_keypair.json",
        help="Secret file for KEYPAIR conversion"
    )
    args = parser.parse_args()

    if args.process == "export":
        verify_aws_auth(ACCOUNT_ID)
        export_dashboards()
    elif args.process == "clean":
        clean_zips(
            args.input_folder,
            env=args.env,
            target_vpc_arn=args.target_vpc_arn,
            override_file=args.override_file,
        )
    elif args.process == "import":
        verify_aws_auth(IMPORT_ACCOUNT_ID, profile=args.profile)
        import_dashboards(args.input_folder, override_file=args.override_file, profile=args.profile)
    elif args.process == "extract-datasources":
        datasource_ids = extract_datasource_ids_from_override(None)
        save_datasource_ids(datasource_ids, args.datasource_ids_file)
    elif args.process == "convert-to-keypair":
        verify_aws_auth(IMPORT_ACCOUNT_ID, profile=args.profile)
        convert_datasources_to_keypair(args.datasource_ids_file, args.secret_file, args.env)
