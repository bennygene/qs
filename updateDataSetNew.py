import json
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def main():
    # OPTIONAL BUT RECOMMENDED in corporate SSL environments
    # Uncomment if needed
    #
    # os.environ["AWS_CA_BUNDLE"] = r"C:\path\to\fiserve-ca-bundle.cer"
    # os.environ["REQUESTS_CA_BUNDLE"] = os.environ["AWS_CA_BUNDLE"]
    # os.environ["SSL_CERT_FILE"] = os.environ["AWS_CA_BUNDLE"]

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

    json_path = out_dir / f"dataset_{dataset_id}_updated.json"

    if not json_path.exists():
        raise FileNotFoundError(f"Dataset JSON not found: {json_path}")

    # Load dataset definition
    with json_path.open("r", encoding="utf-8") as f:
        dataset_payload = json.load(f)

    DISABLE_SSL_VERIFY = True

    # Create QuickSight client
    qs = boto3.client("quicksight", region_name=region,
            verify=not DISABLE_SSL_VERIFY)

    try:
        print("Updating QuickSight dataset:", dataset_id)

        response = qs.update_data_set(
            AwsAccountId=aws_account_id,
            DataSetId=dataset_id,
            Name=dataset_payload["Name"],
            PhysicalTableMap=dataset_payload["PhysicalTableMap"],
            LogicalTableMap=dataset_payload.get("LogicalTableMap", {}),
            ImportMode=dataset_payload["ImportMode"]
            #,ColumnGroups=dataset_payload.get("ColumnGroups", []),
            #,RowLevelPermissionDataSet=dataset_payload.get("RowLevelPermissionDataSet")
            #,RowLevelPermissionTagConfiguration=dataset_payload.get("RowLevelPermissionTagConfiguration")
            #,Permissions=dataset_payload.get("Permissions", []),
        )

        print("✅ Dataset updated successfully")
        print("ARN:", response["Arn"])

    except ClientError as e:
        print("❌ Update failed")
        raise e


if __name__ == "__main__":
    main()
