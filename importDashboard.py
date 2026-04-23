# -------------------- importDashboard.py --------------------
import boto3
import time
import argparse
import json
import os
import random
import string
from datetime import datetime
import sys

def get_quicksight_client(region, profile=None):
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
        return session.client('quicksight', verify=False)
    return boto3.client('quicksight', region_name=region, verify=False)

def read_asset_bundle(asset_bundle_file):
    try:
        with open(asset_bundle_file, 'rb') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading asset bundle file: {e}")
        sys.exit(1)

def load_override_file(override_filepath):
    try:
        with open(override_filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading override file: {e}")
        sys.exit(1)

def clean_override_parameters(override_parameters):
    if not isinstance(override_parameters, dict):
        return override_parameters
    new_override = override_parameters.copy()
    if 'DataSources' in new_override:
        cleaned_data_sources = []
        for ds in new_override['DataSources']:
            if isinstance(ds, dict):
                cleaned_ds = {k: v for k, v in ds.items() if k != "Type" and v is not None}
                cleaned_data_sources.append(cleaned_ds)
        new_override['DataSources'] = cleaned_data_sources
    return new_override

def generate_dynamic_job_id(seed="import"):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{seed}-{timestamp}-{random_suffix}"

def start_import_job(account_id, region, asset_bundle_file, override_file, job_id=None, profile=None):
    client = get_quicksight_client(region, profile)
    asset_bytes = read_asset_bundle(asset_bundle_file)
    override_parameters = load_override_file(override_file)
    override_parameters = clean_override_parameters(override_parameters)

    if not job_id:
        job_id = generate_dynamic_job_id()
    print(f"Using import job ID: {job_id}")

    try:
        response = client.start_asset_bundle_import_job(
            AwsAccountId=account_id,
            AssetBundleImportJobId=job_id,
            AssetBundleImportSource={"Body": asset_bytes},
            OverrideParameters=override_parameters
        )
        print(f"Started import job with ID: {job_id}")
        return job_id
    except Exception as e:
        print(f"Error starting import job: {e}")
        sys.exit(1)

def monitor_import_job(account_id, region, job_id, dashboard_name=None, dashboard_id=None, profile=None):
    client = get_quicksight_client(region, profile)
    try:
        while True:
            response = client.describe_asset_bundle_import_job(
                AwsAccountId=account_id,
                AssetBundleImportJobId=job_id
            )
            status = response.get('JobStatus')
            print(f"[Import Status] Dashboard: {dashboard_name} | ID: {dashboard_id} | Status: {status}")
            if status in ['SUCCESSFUL', 'FAILED', 'FAILED_ROLLBACK_ERROR', 'FAILED_ROLLBACK_IN_PROGRESS', 'FAILED_ROLLBACK_COMPLETED']:
                if status != 'SUCCESSFUL':
                    print("Import job failed. Error details:")
                    errors = response.get('Errors', {})
                    if errors:
                        for error in errors:
                            message = error.get("Message", "No message provided")
                            error_type = error.get("Type", "Unknown")
                            asset_type = error.get("AssetType", "Unknown")
                            asset_id = error.get("AssetId", "Unknown")
                            print(f"  - Type: {error_type}")
                            print(f"    Asset Type: {asset_type}")
                            print(f"    Asset ID: {asset_id}")
                            print(f"    Message: {message}")
                    else:
                        print("No detailed error information returned.")
                    print(json.dumps(errors, indent=4))
                    sys.exit(1)
                else:
                    print("Import job completed successfully.")
                return
            time.sleep(10)
    except Exception as e:
        print(f"Error monitoring import job: {e}")
        sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description="Import QuickSight asset bundle with a dynamic import job ID.")
    parser.add_argument("--account-id", type=str, required=True)
    parser.add_argument("--region", type=str, default="us-east-1")
    parser.add_argument("--asset-bundle", type=str, required=True)
    parser.add_argument("--override", type=str, required=True)
    parser.add_argument("--job-id", type=str, required=False)
    parser.add_argument("--dashboard-id", type=str, required=False)
    parser.add_argument("--profile", type=str, required=False)
    return parser.parse_args()

def main():
    args = parse_args()
    print("Starting QuickSight asset bundle import...")
    job_id = start_import_job(
        args.account_id,
        args.region,
        args.asset_bundle,
        args.override,
        args.job_id,
        profile=args.profile
    )

    monitor_import_job(
        args.account_id,
        args.region,
        job_id,
        dashboard_name=args.job_id,
        dashboard_id=args.dashboard_id,
        profile=args.profile
    )

if __name__ == "__main__":
    main()

