#!/usr/bin/env python3
import boto3
import argparse
import json
from datetime import datetime
from botocore.exceptions import ClientError

# ----------------------------
# Helpers
# ----------------------------
def custom_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def get_quicksight_client(region, profile=None, insecure=False):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("quicksight", region_name=region, verify=(not insecure))

def load_dataset_definition(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Case 1: Raw AWS CLI describe-data-set output
    if "DescribeDataSet" in data:
        return data["DescribeDataSet"]["DataSet"]

    # Case 2: Partially flattened
    if "DataSet" in data:
        return data["DataSet"]

    # Case 3: Already the dataset definition itself
    if "PhysicalTableMap" in data and "ImportMode" in data:
        return data

    raise ValueError(
        "Unrecognized dataset definition format. "
        "Expected DescribeDataSet, DataSet, or direct dataset object."
    )

# ----------------------------
# Update Logic
# ----------------------------
def update_dataset(account_id, region, dataset_id, dataset_def,
                   profile=None, insecure=False, new_import_mode=None):
    """
    Performs QuickSight UpdateDataSet.
    """
    qs = get_quicksight_client(region, profile, insecure)

    # Optional controlled modification
    if new_import_mode:
        print(f"[INFO] Changing ImportMode → {new_import_mode}")
        dataset_def["ImportMode"] = new_import_mode

    try:
        response = qs.update_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id,
            Name=dataset_def["Name"],
            PhysicalTableMap=dataset_def["PhysicalTableMap"],
            LogicalTableMap=dataset_def["LogicalTableMap"],
            ImportMode=dataset_def["ImportMode"],
            ColumnGroups=dataset_def.get("ColumnGroups", []),
            FieldFolders=dataset_def.get("FieldFolders", {}),
            RowLevelPermissionDataSet=dataset_def.get("RowLevelPermissionDataSet"),
            ColumnLevelPermissionRules=dataset_def.get("ColumnLevelPermissionRules", [])
        )

        print("[OK] UpdateDataSet submitted successfully")
        print("Status:", response["Status"])
        return True

    except ClientError as e:
        print("[ERROR] update_data_set failed")
        print(e)
        return False

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Update a QuickSight dataset using a saved or live definition"
    )

    parser.add_argument("--account-id", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--dataset-id", required=True)

    parser.add_argument("--profile", default=None)
    parser.add_argument("--insecure", action="store_true")

    parser.add_argument(
        "--definition-json",
        help="Path to JSON file previously saved by describeDataSet.py",
        required=True
    )

    parser.add_argument(
        "--set-import-mode",
        choices=["SPICE", "DIRECT_QUERY"],
        help="Optionally update ImportMode"
    )

    args = parser.parse_args()

    print("[INFO] Loading dataset definition...")
    dataset_def = load_dataset_definition(args.definition_json)

    success = update_dataset(
        account_id=args.account_id,
        region=args.region,
        dataset_id=args.dataset_id,
        dataset_def=dataset_def,
        profile=args.profile,
        insecure=args.insecure,
        new_import_mode=args.set_import_mode
    )

    if not success:
        print("[FATAL] Dataset update failed")
        return

    print("✅ Dataset update completed")

if __name__ == "__main__":
    main()
