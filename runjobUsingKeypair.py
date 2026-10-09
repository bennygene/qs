import subprocess
import argparse
import os
import json
import zipfile

DASHBOARD_IDS_FILE = "dashboard_ids.txt"  # Path to your dashboard IDs file
DASHBOARD_SUCCESS_FILE = "dashboard_id_success.txt"
DATASOURCE_IDS_FILE = "datasource_ids_migrated.txt"

ACCOUNT_ID = "841162682218"
#region = "us-east-1", "eu-central-1","ap-south-1","ap-southeast-2","eu-west-1"
REGION = "us-east-1"
FOLDER_PATH = "./quicksight/downloads"
EXPORT_SCRIPT = "exportDashboardNEW.py"
CLEAN_SCRIPT = "cleanZIP.py"
IMPORT_SCRIPT = "importDashboardNEW.py"
UPDATE_KEYPAIR_SCRIPT = "run_updateDataSourceWithKeyPair.py"
OVERRIDE_FILE = "dr_override_841162682218_us-east-1_to_430118841756_us-west-2.json"
IMPORT_ACCOUNT_ID = "430118841756"
IMPORT_REGION = "us-west-2"
PROFILE = "target"
#target=dev,cat,prod
DEFAULT_ENV = "prod"
 
def get_success_dashboard_ids():
    if not os.path.exists(DASHBOARD_SUCCESS_FILE):
        return set()
    with open(DASHBOARD_SUCCESS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())
 
def add_success_dashboard_id(dashboard_id):
    with open(DASHBOARD_SUCCESS_FILE, "a", encoding="utf-8") as f:
        f.write(dashboard_id + "\n")

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

def convert_datasources_to_keypair(datasource_ids_file=DATASOURCE_IDS_FILE, secret_file="datasource_secret_arn_keypair.json"):
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
            "--data-source-id", ds_id
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
        export_dashboards()
    elif args.process == "clean":
        clean_zips(args.input_folder, env=args.env)
    elif args.process == "import":
        import_dashboards(args.input_folder, override_file=args.override_file, profile=args.profile)
    elif args.process == "extract-datasources":
        datasource_ids = extract_datasource_ids_from_override(None)
        save_datasource_ids(datasource_ids, args.datasource_ids_file)
    elif args.process == "convert-to-keypair":
        convert_datasources_to_keypair(args.datasource_ids_file, args.secret_file)
