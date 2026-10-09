import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.exceptions import SSLError as BotoSSLError


def parse_args():
    parser = argparse.ArgumentParser(
        description="List failed QuickSight DataSet refreshes (ingestions)."
    )
    parser.add_argument("--account-id", required=True, help="AWS account ID")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Lookback window in hours (default: 24)",
    )
    parser.add_argument(
        "--ca-bundle",
        default="",
        help="Path to corporate/root CA bundle PEM file for SSL validation",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification (testing only)",
    )
    parser.add_argument(
        "--s3-bucket-arn",
        default="arn:aws:s3:::aws-quick-s3-559050241748-us-east-1-an",
        help="S3 bucket ARN where JSON output should be uploaded",
    )
    parser.add_argument(
        "--s3-key",
        default="quicksight/failed_dataset_refreshes.json",
        help="S3 object key for JSON output",
    )
    parser.add_argument(
        "--output-mode",
        choices=["general", "timestamp", "both"],
        default="general",
        help=(
            "S3 upload mode: general=stable key only, timestamp=timestamped key only, "
            "both=timestamped key plus stable key"
        ),
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading JSON to S3 and only print results",
    )
    parser.add_argument(
        "--download-output",
        action="store_true",
        help="Save JSON output to Downloads folder",
    )
    return parser.parse_args()


def bucket_name_from_arn_or_name(bucket_arn_or_name):
    if bucket_arn_or_name.startswith("arn:aws:s3:::"):
        return bucket_arn_or_name.split(":::", 1)[1]
    return bucket_arn_or_name


def upload_json_to_s3(s3_client, bucket_name, key, payload):
    body = json.dumps(payload, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def timestamped_key(base_key, now_utc):
    ts = now_utc.strftime("%Y%m%dT%H%M%SZ")
    if "." in base_key:
        left, right = base_key.rsplit(".", 1)
        return f"{left}_{ts}.{right}"
    return f"{base_key}_{ts}"


def list_all_datasets(qs_client, account_id):
    paginator = qs_client.get_paginator("list_data_sets")
    for page in paginator.paginate(AwsAccountId=account_id):
        for ds in page.get("DataSetSummaries", []):
            yield ds["DataSetId"], ds["Name"]


def list_failed_ingestions(qs_client, account_id, dataset_id, dataset_name, since_utc):
    failed_rows = []
    paginator = qs_client.get_paginator("list_ingestions")

    for page in paginator.paginate(AwsAccountId=account_id, DataSetId=dataset_id):
        for ingestion in page.get("Ingestions", []):
            if ingestion.get("IngestionStatus") != "FAILED":
                continue

            created_time = ingestion["CreatedTime"].astimezone(timezone.utc)
            if created_time < since_utc:
                continue

            ingestion_id = ingestion.get("IngestionId", "")
            error_type = ""
            error_message = ""

            try:
                detail = qs_client.describe_ingestion(
                    AwsAccountId=account_id,
                    DataSetId=dataset_id,
                    IngestionId=ingestion_id,
                )
                error_info = detail.get("Ingestion", {}).get("ErrorInfo", {})
                error_type = error_info.get("Type", "")
                error_message = error_info.get("Message", "")
            except ClientError as exc:
                error_type = "DescribeIngestionError"
                error_message = str(exc)

            failed_rows.append(
                {
                    "dataset_name": dataset_name,
                    "dataset_id": dataset_id,
                    "ingestion_id": ingestion_id,
                    "created_time_utc": created_time.isoformat(),
                    "error_type": error_type,
                    "error_message": error_message,
                }
            )

    return failed_rows


def main():
    args = parse_args()
    verify_value = True
    if args.ca_bundle:
        verify_value = args.ca_bundle
    if args.no_verify_ssl:
        verify_value = False

    qs_client = boto3.client(
        "quicksight",
        region_name=args.region,
        verify=verify_value,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
    s3_client = None
    if not args.no_upload:
        s3_client = boto3.client(
            "s3",
            region_name=args.region,
            verify=verify_value,
            config=Config(retries={"max_attempts": 10, "mode": "standard"}),
        )
    since_utc = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    failed_ingestions = []

    try:
        for dataset_id, dataset_name in list_all_datasets(qs_client, args.account_id):
            failed_ingestions.extend(
                list_failed_ingestions(
                    qs_client,
                    args.account_id,
                    dataset_id,
                    dataset_name,
                    since_utc,
                )
            )
    except BotoSSLError as exc:
        print("SSL validation failed while connecting to QuickSight.")
        print(f"Details: {exc}")
        print("Try one of the following:")
        print("1) Provide corporate CA bundle: --ca-bundle C:/path/to/corp-ca.pem")
        print("2) For temporary testing only: --no-verify-ssl")
        print("3) Set env var AWS_CA_BUNDLE to your CA bundle PEM path")
        return

    failed_ingestions.sort(
        key=lambda r: (r["created_time_utc"], r["dataset_id"], r["ingestion_id"]),
        reverse=True,
    )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "account_id": args.account_id,
        "region": args.region,
        "lookback_hours": args.hours,
        "row_count": len(failed_ingestions),
        "rows": failed_ingestions,
    }

    if args.download_output:
        downloads_folder = Path.home() / "Downloads"
        downloads_folder.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_file = downloads_folder / f"failed_dataset_refreshes_{args.account_id}_{ts}.json"
        with open(output_file, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"Downloaded JSON to {output_file}")

    if args.no_upload:
        print("No-upload mode enabled; skipping S3 upload.")
    else:
        bucket_name = bucket_name_from_arn_or_name(args.s3_bucket_arn)
        keys_to_upload = [args.s3_key]
        if args.output_mode in ("timestamp", "both"):
            ts_key = timestamped_key(args.s3_key, datetime.now(timezone.utc))
            keys_to_upload = [ts_key]
        if args.output_mode == "both":
            keys_to_upload.append(args.s3_key)

        try:
            for key in keys_to_upload:
                upload_json_to_s3(s3_client, bucket_name, key, payload)
                print(f"Uploaded JSON to s3://{bucket_name}/{key}")
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "Unknown")
            error_message = exc.response.get("Error", {}).get("Message", str(exc))
            print("Failed to upload JSON to S3.")
            print(f"Error code: {error_code}")
            print(f"Details: {error_message}")
            print("Try one of the following:")
            print("1) Re-run with --no-upload to skip S3 upload")
            print("2) Use --download-output to save the JSON locally")
            print("3) Upload to a bucket/path your role can write to")
            print("4) Ask the bucket owner to allow s3:PutObject for your role")
            return
        except BotoSSLError as exc:
            print("SSL validation failed while uploading JSON to S3.")
            print(f"Details: {exc}")
            print("Try one of the following:")
            print("1) Provide corporate CA bundle: --ca-bundle C:/path/to/corp-ca.pem")
            print("2) For temporary testing only: --no-verify-ssl")
            print("3) Set env var AWS_CA_BUNDLE to your CA bundle PEM path")
            return

    if not failed_ingestions:
        print(f"No failed DataSet refreshes in the last {args.hours} hours.")
        return

    print(f"Failed DataSet refreshes in the last {args.hours} hours: {len(failed_ingestions)}")
    print("-" * 100)
    for row in failed_ingestions:
        print(f"DataSet Name : {row['dataset_name']}")
        print(f"DataSet ID   : {row['dataset_id']}")
        print(f"Ingestion ID : {row['ingestion_id']}")
        print(f"Created UTC  : {row['created_time_utc']}")
        print(f"Error Type   : {row['error_type']}")
        print(f"Error Message: {row['error_message']}")
        print("-" * 100)


if __name__ == "__main__":
    main()
