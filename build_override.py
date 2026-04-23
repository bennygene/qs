import json
import boto3
import re
import datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# AWS account/profile/region setup
SOURCE_PROFILE = "source"
TARGET_PROFILE = "target"
#SOURCE_REGION = "ap-south-1"
SOURCE_REGION = "eu-central-1"
TARGET_REGION = "eu-central-1"
#TARGET_REGION = "us-east-1"
#TARGET_REGION = "ap-south-1"
SOURCE_ACCOUNT_ID = "748377765571"
TARGET_ACCOUNT_ID = "173294455309"

def list_vpc_connections(profile, account_id, region):
    session = boto3.Session(profile_name=profile, region_name=region)
    qs = session.client("quicksight", verify=False)
    response = qs.list_vpc_connections(AwsAccountId=account_id)
    print(f"Raw response for {profile} in {region}: {json.dumps(response, indent=2, default=str)}")
    return response.get("VPCConnectionSummaries") or response.get("VPCConnections", [])

def list_data_sources(profile, account_id, region):
    session = boto3.Session(profile_name=profile, region_name=region)
    qs = session.client("quicksight", verify=False)
    response = qs.list_data_sources(AwsAccountId=account_id)
    return response.get("DataSources", [])

# Get VPC Connections from both accounts
source_vpcs = list_vpc_connections(SOURCE_PROFILE, SOURCE_ACCOUNT_ID, SOURCE_REGION)
target_vpcs = list_vpc_connections(TARGET_PROFILE, TARGET_ACCOUNT_ID, TARGET_REGION)

# Use the first target VPCConnectionId for all DataSources (or match by name if needed)
if not target_vpcs:
    raise Exception("No VPC connections found in target account!")
target_vpc_id = target_vpcs[0]["VPCConnectionId"]
target_vpc_arn = f"arn:aws:quicksight:{TARGET_REGION}:{TARGET_ACCOUNT_ID}:vpcConnection/{target_vpc_id}"

# Build override file: pair up by index, use VPCConnectionId from source, rest from target
override_vpcs = []
for i in range(min(len(source_vpcs), len(target_vpcs))):
    src = source_vpcs[i]
    tgt = target_vpcs[i]
    subnet_ids = [ni["SubnetId"] for ni in tgt.get("NetworkInterfaces", [])]
    dns_resolvers = tgt.get("DnsResolvers") or []
    override_entry = {
        "VPCConnectionId": src.get("VPCConnectionId"),
        "Name": tgt.get("Name"),
        "SubnetIds": subnet_ids,
        "SecurityGroupIds": tgt.get("SecurityGroupIds"),
        "DnsResolvers": dns_resolvers
    }
    override_vpcs.append(override_entry)

# Get DataSources from both accounts
source_datasources = list_data_sources(SOURCE_PROFILE, SOURCE_ACCOUNT_ID, SOURCE_REGION)
target_datasources = list_data_sources(TARGET_PROFILE, TARGET_ACCOUNT_ID, TARGET_REGION)

# Build a lookup for target datasources by DataSourceId
target_ds_lookup = {ds["DataSourceId"]: ds for ds in target_datasources}

override_datasources = []

for src in source_datasources:
    src_id = src.get("DataSourceId")
    tgt = target_ds_lookup.get(src_id)
    if tgt:
        ds_params = tgt.get("DataSourceParameters", {})
        # Remove AuthenticationType from SnowflakeParameters if present
        if "SnowflakeParameters" in ds_params:
            snowflake_params = ds_params["SnowflakeParameters"]
            if "AuthenticationType" in snowflake_params:
                snowflake_params.pop("AuthenticationType")
        use_fiservadmin = (
            ("RdsParameters" in ds_params and ds_params["RdsParameters"].get("InstanceId")) or
            ("AuroraPostgreSqlParameters" in ds_params and ds_params["AuroraPostgreSqlParameters"].get("InstanceId"))
        )
        override_entry = {
            "DataSourceId": src_id,
            "Name": tgt.get("Name"),
            "DataSourceParameters": ds_params,
        }
        if use_fiservadmin:
            override_entry["Credentials"] = {
                "CredentialPair": {
                    "Username": "fiservadmin",
                    "Password": "fiservadmin"
                }
            }
        else:
            override_entry["Credentials"] = {
                "CredentialPair": {
                    "Username": "CRAPP_UI",
                    "Password": "G$v&oGuEwi9L"
                }
            }
        if "S3Parameters" not in ds_params:
            override_entry["VpcConnectionProperties"] = {
                "VpcConnectionArn": target_vpc_arn
            }
        override_datasources.append(override_entry)
    else:
        ds_params = src.get("DataSourceParameters", {})
        if "SnowflakeParameters" in ds_params:
            snowflake_params = ds_params["SnowflakeParameters"]
            if "AuthenticationType" in snowflake_params:
                snowflake_params.pop("AuthenticationType")
        use_fiservadmin = (
            ("RdsParameters" in ds_params and ds_params["RdsParameters"].get("InstanceId")) or
            ("AuroraPostgreSqlParameters" in ds_params and ds_params["AuroraPostgreSqlParameters"].get("InstanceId"))
        )
        override_entry = {
            "DataSourceId": src_id,
            "Name": src.get("Name"),
            "DataSourceParameters": ds_params,
        }
        if use_fiservadmin:
            override_entry["Credentials"] = {
                "CredentialPair": {
                    "Username": "fiservadmin",
                    "Password": "fiservadmin"
                }
            }
        else:
            override_entry["Credentials"] = {
                "CredentialPair": {
                    "Username": "CRAPP_UI",
                    "Password": "G$v&oGuEwi9L"
                }
            }
        if "S3Parameters" not in ds_params:
            override_entry["VpcConnectionProperties"] = {
                "VpcConnectionArn": target_vpc_arn
            }
        override_datasources.append(override_entry)

# Remove S3Parameters datasources from override_datasources
override_datasources = [
    ds for ds in override_datasources
    if not (
        "DataSourceParameters" in ds and
        "S3Parameters" in ds["DataSourceParameters"]
    )
]

override_data = {
    "VPCConnections": override_vpcs,
    "DataSources": override_datasources
}

with open("override_built_aura-dev-aura-uat.json", "w") as f:
    json.dump(override_data, f, indent=4, default=json_serial)

print("Override file created as override_built_mrmnaint-dev-mrmlatamint-dev.json")
