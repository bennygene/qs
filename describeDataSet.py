#!/usr/bin/env python3
import boto3
import argparse
import json
import os
from datetime import datetime
from botocore.exceptions import ClientError

def custom_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def get_quicksight_client(region, profile=None, verify=False):
    """
    Creates a QuickSight client.
    - profile: AWS profile name (optional)
    - insecure: if True, disables SSL cert verification (matches your existing behavior)
    """
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.client("quicksight", region_name=region, verify=False)

def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def describe_dataset(account_id, region, dataset_id, profile=None, insecure=False):
    qs = get_quicksight_client(region, profile=profile, verify=False)
    try:
        resp = qs.describe_data_set(AwsAccountId=account_id, DataSetId=dataset_id)
        return resp
    except ClientError as e:
        print(f"[ERROR] describe_data_set failed: {e}")
        return None

def describe_dataset_permissions(account_id, region, dataset_id, profile=None, insecure=False):
    qs = get_quicksight_client(region, profile=profile, verify=False)
    try:
        resp = qs.describe_data_set_permissions(AwsAccountId=account_id, DataSetId=dataset_id)
        return resp
    except ClientError as e:
        print(f"[WARN] describe_data_set_permissions failed: {e}")
        return None

def list_dataset_tags(region, dataset_arn, profile=None, insecure=False):
    qs = get_quicksight_client(region, profile=profile, verify=False)
    try:
        resp = qs.list_tags_for_resource(ResourceArn=dataset_arn)
        return resp
    except ClientError as e:
        print(f"[WARN] list_tags_for_resource failed: {e}")
        return None

def summarize_dataset(ds_resp, perm_resp=None, tags_resp=None):
    ds = ds_resp.get("DataSet", {})
    name = ds.get("Name")
    arn = ds.get("Arn")
    import_mode = ds.get("ImportMode")  # SPICE or DIRECT_QUERY
    created = ds.get("CreatedTime")
    updated = ds.get("LastUpdatedTime")

    physical_map = ds.get("PhysicalTableMap", {}) or {}
    logical_map  = ds.get("LogicalTableMap", {}) or {}
    output_cols  = ds.get("OutputColumns", []) or []

    rls = ds.get("RowLevelPermissionDataSet")
    col_level = ds.get("ColumnLevelPermissionRules", [])

    print("\n==================== QuickSight DataSet Summary ====================")
    print(f"Name           : {name}")
    print(f"DataSetId      : {ds.get('DataSetId')}")
    print(f"Arn            : {arn}")
    print(f"ImportMode     : {import_mode}")
    print(f"CreatedTime    : {created}")
    print(f"LastUpdatedTime: {updated}")
    print("--------------------------------------------------------------------")
    print(f"PhysicalTables : {len(physical_map)}")
    print(f"LogicalTables  : {len(logical_map)}")
    print(f"OutputColumns  : {len(output_cols)}")
    print("--------------------------------------------------------------------")

    if physical_map:
        print("\n[Physical Tables]")
        for pt_id, pt_def in physical_map.items():
            # Physical tables can be RelationalTable, CustomSql, S3Source, etc.
            kind = next(iter(pt_def.keys()), "Unknown")
            print(f" - {pt_id} ({kind})")
            rel = pt_def.get("RelationalTable")
            csql = pt_def.get("CustomSql")
            s3   = pt_def.get("S3Source")
            if rel:
                print(f"    DataSourceArn: {rel.get('DataSourceArn')}")
                print(f"    Schema/Table : {rel.get('Schema')} / {rel.get('Name')}")
            if csql:
                print(f"    DataSourceArn: {csql.get('DataSourceArn')}")
                print(f"    CustomSqlName: {csql.get('Name')}")
                print(f"    SqlQuery     : {('[omitted]' if csql.get('SqlQuery') else None)}")
            if s3:
                print(f"    DataSourceArn: {s3.get('DataSourceArn')}")
                print(f"    InputColumns : {len(s3.get('InputColumns', []) or [])}")

    if logical_map:
        print("\n[Logical Tables]")
        for lt_id, lt_def in logical_map.items():
            alias = safe_get(lt_def, "Alias")
            source = safe_get(lt_def, "Source", default={})
            source_type = next(iter(source.keys()), "Unknown")
            print(f" - {lt_id} (Alias: {alias}, Source: {source_type})")

    if output_cols:
        print("\n[Output Columns] (first 30 shown)")
        for col in output_cols[:30]:
            print(f" - {col.get('Name')} ({col.get('Type')})")
        if len(output_cols) > 30:
            print(f"   ... and {len(output_cols) - 30} more")

    if rls:
        print("\n[Row-Level Security (RLS)]")
        print(json.dumps(rls, indent=2, default=custom_json_serializer))

    if col_level:
        print("\n[Column-Level Permissions]")
        print(f"Rules: {len(col_level)}")

    if perm_resp and perm_resp.get("Permissions") is not None:
        perms = perm_resp.get("Permissions", [])
        print("\n[Permissions]")
        print(f"Entries: {len(perms)}")
        for p in perms[:30]:
            principal = p.get("Principal")
            actions = p.get("Actions", [])
            print(f" - Principal: {principal}")
            print(f"   Actions  : {len(actions)}")
        if len(perms) > 30:
            print(f"   ... and {len(perms) - 30} more permission entries")

    if tags_resp and tags_resp.get("Tags") is not None:
        tags = tags_resp.get("Tags", [])
        print("\n[Tags]")
        if not tags:
            print(" - (none)")
        else:
            for t in tags:
                print(f" - {t.get('Key')} = {t.get('Value')}")

    print("\n====================================================================\n")

def main():
    parser = argparse.ArgumentParser(
        description="Describe a QuickSight dataset (definition + optional permissions/tags)."
    )
    parser.add_argument("--account-id", required=True, type=str)
    parser.add_argument("--region", default="us-east-1", type=str)
    parser.add_argument("--dataset-id", required=True, type=str)

    parser.add_argument("--profile", required=False, type=str, default=None,
                        help="AWS profile name (optional)")
    parser.add_argument("--insecure", action="store_true",
                        help="Disable SSL verification (mirrors verify=False in your script)")

    parser.add_argument("--include-permissions", action="store_true",
                        help="Also call DescribeDataSetPermissions")
    parser.add_argument("--include-tags", action="store_true",
                        help="Also call ListTagsForResource (requires dataset ARN)")

    parser.add_argument("--save-json", required=False, type=str, default=None,
                        help="Path to save full JSON response (merged)")

    args = parser.parse_args()

    ds_resp = describe_dataset(
        args.account_id, args.region, args.dataset_id,
        profile=args.profile, insecure=args.insecure
    )
    if not ds_resp:
        print("[FATAL] Unable to describe dataset.")
        return

    dataset_arn = safe_get(ds_resp, "DataSet", "Arn")
    perm_resp = None
    tags_resp = None

    if args.include_permissions:
        perm_resp = describe_dataset_permissions(
            args.account_id, args.region, args.dataset_id,
            profile=args.profile, insecure=args.insecure
        )

    if args.include_tags and dataset_arn:
        tags_resp = list_dataset_tags(
            args.region, dataset_arn,
            profile=args.profile, insecure=args.insecure
        )

    summarize_dataset(ds_resp, perm_resp=perm_resp, tags_resp=tags_resp)

    if args.save_json:
        merged = {
            "DescribeDataSet": ds_resp,
            "DescribeDataSetPermissions": perm_resp,
            "ListTagsForResource": tags_resp
        }
        os.makedirs(os.path.dirname(args.save_json) or ".", exist_ok=True)
        with open(args.save_json, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, default=custom_json_serializer)
        print(f"[OK] Saved JSON to: {args.save_json}")

if __name__ == "__main__":
    main()
