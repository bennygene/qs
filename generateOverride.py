
import argparse
import json
import boto3
import re
import configparser

def list_vpc_connections(profile, account_id, region):
    session = boto3.Session(profile_name=profile, region_name=region)
    qs = session.client("quicksight", verify=False)
    response = qs.list_vpc_connections(AwsAccountId=account_id)
    return response.get("VPCConnectionSummaries") or response.get("VPCConnections", [])

def list_data_sources(profile, account_id, region):
    session = boto3.Session(profile_name=profile, region_name=region)
    qs = session.client("quicksight", verify=False)
    response = qs.list_data_sources(AwsAccountId=account_id)
    return response.get("DataSources", [])

def get_credentials(env, use_fiservadmin):
    normalized_env = "dev" if env in ["dev", "qa"] else env
    if use_fiservadmin:
        return {"Username": "fiservadmin", "Password": "fiservadmin"}
    if normalized_env == "dev":
        return {"Username": "CRAPP_UI", "Password": "G$v&oGuEwi9L"}
    # added for cat env
    elif normalized_env == "cat":
        return {"Username": "CRAPP_UI", "Password": "*$4Lh^&934SF"}
    elif normalized_env in ["prod", "dr"]:
        return {"Username": "CRAPP_UI", "Password": "naYi!B#*oW4K"}
    else:
        return {"Username": "fiservadmin", "Password": "fiservadmin"}

def transform_token_by_env(token, env):
    token = token.upper()

    env = "dev" if env in ["dev", "qa"] else env.lower()

    if token in {"CRANALYTICS", "CRANALYTICSCAT", "CRANALYTICSDEV"}:
        return {
            "dev": "CRANALYTICSDEV",
            "cat": "CRANALYTICSCAT",
            "prod": "CRANALYTICS"
        }.get(env, token)
    
    elif token in {"CRRISKDB", "CRRISKCATDB", "CRRISKDEVDB", "CRRISKDBDEV"}:
        if env == "dev":
            return "CRRISKDEVDB" if token in {"CRRISKDB", "CRRISKCATDB"} else token
        # added "CRRISKDBDEV"
        elif env == "cat":
            return "CRRISKCATDB" if token in {"CRRISKDB", "CRRISKDEVDB","CRRISKDBDEV"} else token
        elif env == "prod":
            return "CRRISKDB" if token in {"CRRISKDBDEV", "CRRISKCATDB"} else token
        else:
            return token

    elif token in {"CRDATAHUB", "CRDATAHUBDEV", "CRDATAHUBCAT"}:
        return {
            "dev": "CRDATAHUBDEV",
            "cat": "CRDATAHUBCAT",
            "prod": "CRDATAHUB"
        }.get(env, token)

    return token

def transform_snowflake_parameters(params, env):
    if "SnowflakeParameters" in params:
        snowflake = params["SnowflakeParameters"]
        for key in ["Database", "Warehouse", "Host"]:
            if key in snowflake:
                snowflake[key] = transform_token_by_env(snowflake[key], env)
        params["SnowflakeParameters"] = snowflake
    return params

def main():
    parser = argparse.ArgumentParser(description="Generate override file for QuickSight asset bundle import.")
    parser.add_argument("--env", required=True)
    parser.add_argument("--prop-file", required=True)
    parser.add_argument("--source-profile", required=True)
    parser.add_argument("--target-profile", required=True)
    parser.add_argument("--source-region", required=True)
    parser.add_argument("--target-region", required=True)
    parser.add_argument("--source-account-id", required=True)
    parser.add_argument("--target-account-id", required=True)
    parser.add_argument("--output", required=False, help="Output override filename")

    args = parser.parse_args()
    normalized_env = "dev" if args.env in ["dev", "qa"] else args.env

    override_filename = args.output or f"{args.env}_override_{args.source_account_id}_{args.source_region}_to_{args.target_account_id}_{args.target_region}.json"

    source_vpcs = list_vpc_connections(args.source_profile, args.source_account_id, args.source_region)
    target_vpcs = list_vpc_connections(args.target_profile, args.target_account_id, args.target_region)

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
     # print subnet id   
        print("subnet_ids",subnet_ids)
        override_vpcs.append(override_entry)

    source_datasources = list_data_sources(args.source_profile, args.source_account_id, args.source_region)
    target_datasources = list_data_sources(args.target_profile, args.target_account_id, args.target_region)
    target_ds_lookup = {ds["DataSourceId"]: ds for ds in target_datasources}

    override_datasources = []
    for src in source_datasources:
        src_id = src.get("DataSourceId")
        tgt = target_ds_lookup.get(src_id)
        ds_params = (tgt or src).get("DataSourceParameters", {})

        ds_params = transform_snowflake_parameters(ds_params, args.env)

        if "SnowflakeParameters" in ds_params:
            ds_params["SnowflakeParameters"].pop("AuthenticationType", None)

        use_fiservadmin = (
            ("RdsParameters" in ds_params and ds_params["RdsParameters"].get("InstanceId")) or
            ("AuroraPostgreSqlParameters" in ds_params and ds_params["AuroraPostgreSqlParameters"].get("InstanceId"))
        )
        credentials = get_credentials(args.env, use_fiservadmin)
        override_entry = {
            "DataSourceId": src_id,
            "Name": (tgt or src).get("Name"),
            "DataSourceParameters": ds_params,
            "Credentials": {
                "CredentialPair": credentials
            }
        }
        if "S3Parameters" not in ds_params:
            vpc_props = (tgt or src).get("VpcConnectionProperties")
            if vpc_props:
                override_entry["VpcConnectionProperties"] = vpc_props
        override_datasources.append(override_entry)

    override_datasources = [
        ds for ds in override_datasources
        if "S3Parameters" not in ds.get("DataSourceParameters", {})
    ][:50]

    for ds in override_datasources:
        vpc_props = ds.get("VpcConnectionProperties")
        if vpc_props and "VpcConnectionArn" in vpc_props:
            arn = vpc_props["VpcConnectionArn"]
            arn = re.sub(
                r"arn:aws:quicksight:[^:]+:[^:]+:",
                f"arn:aws:quicksight:{args.target_region}:{args.target_account_id}:",
                arn
            )
            vpc_props["VpcConnectionArn"] = arn

    override_data = {
        "VPCConnections": override_vpcs,
        "DataSources": override_datasources
    }

    with open(override_filename, "w") as f:
        json.dump(override_data, f, indent=4)

    print(f"Override file created as {override_filename}")

    config = configparser.ConfigParser()
    config.read(args.prop_file)

    if args.env not in config:
        raise ValueError(f"Environment '{args.env}' not found in property file.")

    env_config = config[args.env]
    target_db = env_config.get("Database")
    target_wh = env_config.get("Warehouse")

    SNOWFLAKE_HOSTS = {
        "dev": "fiservgbsdev.us-west-2.privatelink.snowflakecomputing.com",
        "qa":  "fiservgbsdev.us-west-2.privatelink.snowflakecomputing.com",
        "cat": "zvb61939.us-west-2.privatelink.snowflakecomputing.com",
        "uat": "zvb61939.us-west-2.privatelink.snowflakecomputing.com",
        "prod": "fiservgbsprod.us-west-2.privatelink.snowflakecomputing.com",
        "dr":   "fiservgbsprod.us-west-2.privatelink.snowflakecomputing.com"
    }


    target_host = SNOWFLAKE_HOSTS.get(args.env)

    with open(override_filename, "r") as f:
        override_data = json.load(f)

    for ds in override_data.get("DataSources", []):
        params = ds.get("DataSourceParameters", {})
        if "SnowflakeParameters" in params:
            snowflake = params["SnowflakeParameters"]
            if target_host:
                snowflake["Host"] = target_host
            if target_db:
                snowflake["Database"] = target_db
            if target_wh:
                snowflake["Warehouse"] = target_wh

            params["SnowflakeParameters"] = snowflake
            ds["DataSourceParameters"] = params

    with open(override_filename, "w") as f:
        json.dump(override_data, f, indent=4)

    print(f"Final override file updated with target environment '{args.env}' values.")

if __name__ == "__main__":
    main()
