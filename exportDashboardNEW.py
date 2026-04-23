import boto3
import time
import argparse
import json
import os
import requests
from datetime import datetime
 
# Custom JSON serializer for datetime objects
def custom_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")
 
# Initialize QuickSight client
def get_quicksight_client(region):
    return boto3.client('quicksight', verify=False, region_name=region)
 
# Describe a specific dashboard to get its name
def get_dashboard_name(account_id, dashboard_id, region):
    quicksight_client = get_quicksight_client(region)
    try:
        response = quicksight_client.describe_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id
        )
        dashboard_name = response['Dashboard']['Name']
        print(f"Dashboard Name: {dashboard_name}")
        return dashboard_name
    except Exception as e:
        print(f"Error retrieving dashboard name: {e}")
        return None
 
# Start an asset bundle export job for a dashboard or resource ARN
def start_export_job(account_id, region, export_bucket, dashboard_id=None, resource_arn=None):
    quicksight_client = get_quicksight_client(region)
    export_job_id = f"export-job-{int(time.time())}"
    try:
        if resource_arn:
            resource_arns = [resource_arn]
        else:
            resource_arns = [f"arn:aws:quicksight:{region}:{account_id}:dashboard/{dashboard_id}"]
        response = quicksight_client.start_asset_bundle_export_job(
            AwsAccountId=account_id,
            AssetBundleExportJobId=export_job_id,
            ResourceArns=resource_arns,
            ExportFormat='QUICKSIGHT_JSON',
            IncludeAllDependencies=True,
            IncludePermissions=True,
            IncludeTags=True
        )
        print(f"Export job started: {response['AssetBundleExportJobId']}")
        return export_job_id
    except Exception as e:
        print(f"Error starting export job: {e}")
        return None
 
# Monitor the status of an export job and retrieve the S3 URI or error details
def monitor_export_job(account_id, region, export_job_id):
    quicksight_client = get_quicksight_client(region)
    try:
        while True:
            response = quicksight_client.describe_asset_bundle_export_job(
                AwsAccountId=account_id,
                AssetBundleExportJobId=export_job_id
            )
            status = response['JobStatus']
            print(f"Export job status: {status}")
            if status in ['SUCCESSFUL', 'FAILED']:
                if status == 'FAILED':
                    error_info = response.get('Errors', {})
                    if not error_info:
                        print("Export job failed, but no error details were provided.")
                        print(f"Full response: {json.dumps(response, indent=4, default=custom_json_serializer)}")
                    else:
                        print(f"Export job failed. Error details: {json.dumps(error_info, indent=4, default=custom_json_serializer)}")
                elif status == 'SUCCESSFUL':
                    print("Export job completed successfully.")
                return status, response
            time.sleep(10)
    except Exception as e:
        print(f"Error monitoring export job: {e}")
        return None, None
 
# Extract the download URL from the export job response and download the file
def get_download_url(export_response, folder_path, local_file):
    try:
        download_url = export_response['DownloadUrl']
        resource_arns = export_response['ResourceArns']
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        full_file_path = os.path.join(folder_path, local_file)
        print(f"Resource ARN(s): {resource_arns}")
        print(f"Filename to download: {full_file_path}")
        response = requests.get(download_url, stream=True, verify=False)
        if response.status_code == 200:
            with open(full_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"File downloaded successfully to {full_file_path}")
            return 'SUCCESSFUL'
        else:
            print(f"Failed to download file. HTTP status code: {response.status_code}")
    except KeyError as e:
        print(f"Error extracting DownloadUrl or ResourceArns: {e}")
    except Exception as e:
        print(f"Error downloading file: {e}")
 
# Main function
def main():
    parser = argparse.ArgumentParser(description='Export QuickSight dashboard or resource and get download URL.')
    parser.add_argument('--account-id', type=str, required=True, help='The AWS account ID.')
    parser.add_argument('--region', type=str, default='us-east-1', help='The AWS region.')
    parser.add_argument('--dashboard-id', type=str, required=False, help='The ID of the dashboard to export.')
    parser.add_argument('--resource-arn', type=str, required=False, help='The full QuickSight resource ARN to export.')
    parser.add_argument('--folder-path', type=str, required=True, help='Folder path to save the exported dashboard.')
    parser.add_argument('--export-bucket', type=str, required=False, help='The S3 bucket for exporting the dashboard.')
    args = parser.parse_args()
 
    # Determine dashboard name or use resource ARN
    if args.resource_arn:
        dashboard_name = args.resource_arn.split('/')[-1]
        print(f"Using resource ARN: {args.resource_arn}")
    elif args.dashboard_id:
        print("Retrieving dashboard name...")
        dashboard_name = get_dashboard_name(args.account_id, args.dashboard_id, args.region)
        if not dashboard_name:
            print("Failed to retrieve dashboard name.")
            return
    else:
        print("Either --dashboard-id or --resource-arn must be provided.")
        return
 
    local_file = f"{dashboard_name.replace(' ', '_')}.zip"
 
    print("Starting export job...")
    export_job_id = start_export_job(
        args.account_id,
        args.region,
        args.export_bucket,
        dashboard_id=args.dashboard_id,
        resource_arn=args.resource_arn
    )
    if not export_job_id:
        print("Failed to start export job.")
        return
 
    print("Monitoring export job status...")
    export_status, export_response = monitor_export_job(args.account_id, args.region, export_job_id)
    if export_status != 'SUCCESSFUL':
        print("Export job did not complete successfully.")
        return
 
    print("Generating download URL...")
    download_url = get_download_url(export_response, args.folder_path, local_file)
    if download_url:
        print(f"Download URL: {download_url}")
    else:
        print("Failed to generate download URL.")
 
if __name__ == "__main__":
    main()
