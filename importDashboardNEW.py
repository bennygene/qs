#!/usr/bin/env python3
import boto3
import time
import argparse
import json
import os
import random
import string
from datetime import datetime
 
def get_quicksight_client(region, profile=None):
    """
    Returns a QuickSight client for the specified region.
    If a profile is provided, the session uses that profile's credentials.
    """
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
        return session.client('quicksight', verify=False)
    return boto3.client('quicksight', region_name=region, verify=False)
 
def read_asset_bundle(asset_bundle_file):
    """
    Reads the asset bundle file from disk in binary mode.
    """
    try:
        with open(asset_bundle_file, 'rb') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading asset bundle file: {e}")
        return None
 
def load_override_file(override_filepath):
    """
    Loads the override JSON file containing custom override parameters.
    """
    try:
        with open(override_filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading override file: {e}")
        return None
 
def clean_override_parameters(override_parameters):
    """
    Cleans the override parameters by removing disallowed keys and keys with None values from each DataSource.
   
    The API expects each DataSource to only include:
      - DataSourceId
      - Name
      - DataSourceParameters
      - VpcConnectionProperties (if provided; must be a dict)
      - SslProperties
      - Credentials
     
    This function removes the "Type" key and any key whose value is None.
    """
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
 
def generate_dynamic_job_id(dashboard_arn):
    """
    Generates a unique job ID based on the dashboard ARN.
    Extracts the dashboard’s unique identifier (the part after the last '/'),
    appends a timestamp, and adds a random 6-character suffix.
    """
    base_id = dashboard_arn.strip().split("/")[-1]
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{base_id}-{timestamp}-{random_suffix}"
 
def start_import_job(account_id, region, asset_bundle_file, override_file, dashboard_arn, job_id=None, profile=None):
    """
    Starts the QuickSight asset bundle import job using the asset bundle file
    and the override parameters. If no job ID is provided, a dynamic one is generated
    using the provided dashboard ARN.
   
    The API expects the file content under the "Body" key.
    """
    client = get_quicksight_client(region, profile)
    asset_bytes = read_asset_bundle(asset_bundle_file)
    if asset_bytes is None:
        return None
   
    override_parameters = load_override_file(override_file)
    if override_parameters is None:
        return None
 
    # Clean override parameters to remove disallowed keys.
    override_parameters = clean_override_parameters(override_parameters)
 
    if not job_id:
        job_id = generate_dynamic_job_id(dashboard_arn)
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
        return None
 
def monitor_import_job(account_id, region, job_id, profile=None):
    """
    Polls the status of the import job until it completes (either 'SUCCESSFUL' or 'FAILED'),
    using a fixed 10-second interval between polls.
    """
    client = get_quicksight_client(region, profile)
    try:
        while True:
            response = client.describe_asset_bundle_import_job(
                AwsAccountId=account_id,
                AssetBundleImportJobId=job_id
            )
            status = response.get('JobStatus')
            print(f"Import job status: {status}")
            if status == 'FAILED_ROLLBACK_COMPLETED':
                errors = response.get('Errors', {})
                print("Import job failed. Error details:")
                print(json.dumps(errors, indent=4))
                return status, response  # Exit immediately on failure
            elif status == 'SUCCESSFUL':
                print("Import job completed successfully.")
                return status, response
            time.sleep(10)
    except Exception as e:
        print(f"Error monitoring import job: {e}")
        return None, None
 
def parse_args():
    parser = argparse.ArgumentParser(
        description="Import QuickSight asset bundle with a dynamic import job ID."
    )
    parser.add_argument("--account-id", type=str, required=True,
                        help="AWS account ID for the import job (target account).")
    parser.add_argument("--region", type=str, default="us-east-1",
                        help="AWS region to use (e.g., us-east-1 or ap-south-1).")
    parser.add_argument("--asset-bundle", type=str, required=True,
                        help="Path to the asset bundle file (e.g., exported zip file).")
    parser.add_argument("--override", type=str, required=True,
                        help="Path to the override JSON file (generated by generateOverride).")
    parser.add_argument("--dashboard-arn", type=str, required=True,
                        help="Dashboard ARN used to generate a dynamic import job ID.")
    parser.add_argument("--job-id", type=str, required=False,
                        help="Optional job ID override if you want to manually specify your job ID.")
    parser.add_argument("--profile", type=str, required=False,
                        help="AWS CLI profile name to use (e.g., targetAccount).")
    return parser.parse_args()
 
def main():
    args = parse_args()
    print("Arguments received:")
    print(args)
    job_id = start_import_job(
        args.account_id,    # Correct attribute name
        args.region,
        args.asset_bundle,
        args.override,
        args.dashboard_arn,
        args.job_id,
        profile=args.profile
    )
    if job_id is None:
        print("Failed to start import job.")
        return
    status, response = monitor_import_job(args.account_id, args.region, job_id, profile=args.profile)
    if status == 'SUCCESSFUL':
        print("Asset bundle imported successfully!")
    else:
        print("Asset bundle import encountered issues.")
 
if __name__ == "__main__":
    main()
