
import argparse
import base64
import json
import boto3
import re
import configparser
from pathlib import Path

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


def load_secret_arn_from_file(secret_file_path):
    path = Path(secret_file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Secret file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    secret_arn = payload.get("secret_arn") or payload.get("secretArn") or payload.get("SecretArn")
    if secret_arn:
        return secret_arn

    secret_name = payload.get("secret_name") or payload.get("secretName") or payload.get("SecretName")
    if secret_name:
        return secret_name

    raise ValueError(
        "Secret file must contain 'secret_arn' (or secretArn/SecretArn) "
        "or 'secret_name' (or secretName/SecretName)."
    )


def resolve_secret_id(secret_ref, account_id, region):
    text = str(secret_ref).strip()
    if not text:
        raise ValueError("Secret reference is empty.")

    if text.startswith("arn:aws:secretsmanager:"):
        return text

    return f"arn:aws:secretsmanager:{region}:{account_id}:secret:{text}"


def get_secret_json(profile, secret_arn, default_region):
    arn_parts = str(secret_arn).split(":")
    secret_region = arn_parts[3] if len(arn_parts) > 3 and arn_parts[3] else default_region
    session = boto3.Session(profile_name=profile, region_name=secret_region)
    sm = session.client("secretsmanager", verify=False)
    response = sm.get_secret_value(SecretId=secret_arn)
    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError("SecretString is empty. Expected JSON with key pair fields.")

    secret_json = json.loads(secret_string)
    if not isinstance(secret_json, dict):
        raise ValueError("SecretString JSON must be an object.")

    return secret_json


def normalize_private_key_pem(private_key):
    key_text = str(private_key).strip()
    if not key_text:
        raise ValueError("Private key value is empty.")

    if len(key_text) >= 2 and key_text[0] == key_text[-1] and key_text[0] in ('"', "'"):
        key_text = key_text[1:-1].strip()

    key_text = key_text.replace("\\r\\n", "\n").replace("\\n", "\n")
    key_text = key_text.replace("\r\n", "\n").replace("\r", "\n")

    pem_match = re.search(
        r"(-----BEGIN [A-Z0-9 ]+-----)(.*?)(-----END [A-Z0-9 ]+-----)",
        key_text,
        flags=re.DOTALL,
    )
    if pem_match:
        begin_line, body, end_line = pem_match.group(1), pem_match.group(2), pem_match.group(3)
        begin_label = begin_line.replace("-----BEGIN ", "").replace("-----", "").strip()
        end_label = end_line.replace("-----END ", "").replace("-----", "").strip()
        if begin_label != end_label:
            raise ValueError("Private key PEM block has mismatched BEGIN/END labels.")

        if begin_label == "OPENSSH PRIVATE KEY":
            raise ValueError(
                "OPENSSH private keys are not supported for QuickSight KeyPair credentials. "
                "Convert to PKCS8 PEM (BEGIN PRIVATE KEY)."
            )

        body_compact = "".join(body.split())
        if body_compact and not re.fullmatch(r"[A-Za-z0-9+/=]+", body_compact):
            raise ValueError("Private key PEM body contains non-base64 characters.")

        body_lines = [body_compact[i : i + 64] for i in range(0, len(body_compact), 64)]
        return begin_line + "\n" + "\n".join(body_lines) + "\n" + end_line + "\n"

    compact = "".join(key_text.split()).replace("-", "+").replace("_", "/")
    if not re.fullmatch(r"[A-Za-z0-9+/=]+", compact):
        raise ValueError(
            "Private key must be PEM formatted or base64 DER text. "
            "Expected BEGIN/END markers or base64 key content."
        )

    compact += "=" * ((4 - len(compact) % 4) % 4)
    try:
        base64.b64decode(compact, validate=True)
    except Exception as exc:
        raise ValueError(
            "Private key base64 content is invalid. Provide a valid PEM key or base64 DER text."
        ) from exc

    body_lines = [compact[i : i + 64] for i in range(0, len(compact), 64)]
    return "-----BEGIN PRIVATE KEY-----\n" + "\n".join(body_lines) + "\n-----END PRIVATE KEY-----\n"


def extract_keypair_credentials(secret_json, require_encrypted_key=False):
    username = (
        secret_json.get("username")
        or secret_json.get("key_pair_username")
        or secret_json.get("KeyPairUsername")
    )
    private_key = (
        secret_json.get("private_key")
        or secret_json.get("privateKey")
        or secret_json.get("PrivateKey")
    )
    private_key_passphrase = (
        secret_json.get("private_key_passphrase")
        or secret_json.get("privateKeyPassphrase")
        or secret_json.get("passphrase")
        or secret_json.get("PrivateKeyPassphrase")
    )

    password = secret_json.get("password") or secret_json.get("Password")

    if not username:
        raise ValueError(
            "Secret JSON must include username for Snowflake key pair authentication."
        )

    if not private_key and password:
        raise ValueError(
            "Secret appears to be password-based (contains password, no private_key). "
            "Use a key-pair secret for Snowflake sources."
        )

    if not private_key:
        raise ValueError(
            "Secret JSON must include non-empty private key. Preferred field is 'key'."
        )

    if require_encrypted_key and not private_key_passphrase:
        raise ValueError(
            "UAT encrypted mode requires non-empty 'keyPassphrase' in the secret JSON."
        )

    private_key_text = normalize_private_key_pem(private_key)

    credentials = {
        "KeyPairCredentials": {
            "KeyPairUsername": username,
            "PrivateKey": private_key_text,
        }
    }
    if private_key_passphrase:
        credentials["KeyPairCredentials"]["PrivateKeyPassphrase"] = private_key_passphrase

    return credentials

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

    if token in {"CRANALYTICS", "CRANALYTICSCAT", "CRANALYTICSDEV","CRANALYTICS"}:
        return {
            "dev": "CRANALYTICSDEV",
            "cat": "CRANALYTICSCAT",
            "prod": "CRANALYTICS",
            "dr": "CRANALYTICSDR"
        }.get(env, token)
    
    elif token in {"CRRISKDB", "CRRISKCATDB", "CRRISKDEVDB", "CRRISKDBDEV","CRRISKDBDR"}:
        if env == "dev":
            return "CRRISKDEVDB" if token in {"CRRISKDB", "CRRISKCATDB"} else token
        # added "CRRISKDBDEV"
        elif env == "cat":
            return "CRRISKCATDB" if token in {"CRRISKDB", "CRRISKDEVDB","CRRISKDBDEV"} else token
        elif env == "prod":
            return "CRRISKDB" if token in {"CRRISKDBDEV", "CRRISKCATDB", "CRRISKDBDR"} else token
        elif env == "dr":
            return "CRRISKDBDR" if token in {"CRRISKDB", "CRRISKCATDB", "CRRISKDEVDB", "CRRISKDBDEV"} else token
        else:
            return token

    elif token in {"CRDATAHUB", "CRDATAHUBDEV", "CRDATAHUBCAT", "CRDATAHUBDR"}:
        return {
            "dev": "CRDATAHUBDEV",
            "cat": "CRDATAHUBCAT",
            "prod": "CRDATAHUB",
            "dr": "CRDATAHUBDR"
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
    parser.add_argument(
        "--keypair-secret-arn",
        required=False,
        help="Secret ARN or secret name for Snowflake key pair credentials.",
    )
    parser.add_argument(
        "--keypair-secret-file",
        required=False,
        default=str(Path(__file__).resolve().parent / "datasource_secret_arn_keypair.json"),
        help="Secret ARN mapping JSON file used for Snowflake key pair credentials.",
    )

    args = parser.parse_args()
    normalized_env = "dev" if args.env in ["dev", "qa"] else args.env
    require_encrypted_key = str(args.env).strip().lower() == "uat"

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

    snowflake_secret_arn = None

    override_datasources = []
    for src in source_datasources:
        src_id = src.get("DataSourceId")
        tgt = target_ds_lookup.get(src_id)
        ds_params = (tgt or src).get("DataSourceParameters", {})

        ds_params = transform_snowflake_parameters(ds_params, args.env)

        if "SnowflakeParameters" in ds_params:
            ds_params["SnowflakeParameters"]["AuthenticationType"] = "KEYPAIR"

        use_fiservadmin = (
            ("RdsParameters" in ds_params and ds_params["RdsParameters"].get("InstanceId")) or
            ("AuroraPostgreSqlParameters" in ds_params and ds_params["AuroraPostgreSqlParameters"].get("InstanceId"))
        )

        if "SnowflakeParameters" in ds_params:
            if snowflake_secret_arn is None:
                secret_ref = args.keypair_secret_arn or load_secret_arn_from_file(args.keypair_secret_file)
                secret_arn = resolve_secret_id(secret_ref, args.target_account_id, args.target_region)
                secret_json = get_secret_json(args.target_profile, secret_arn, args.target_region)
                # Validate secret shape; enforce passphrase only for UAT.
                extract_keypair_credentials(secret_json, require_encrypted_key=require_encrypted_key)
                snowflake_secret_arn = secret_arn
                print(f"Loaded Snowflake key pair secret ARN: {secret_arn}")
            credentials_block = {
                "SecretArn": snowflake_secret_arn
            }
        else:
            credentials = get_credentials(args.env, use_fiservadmin)
            credentials_block = {
                "CredentialPair": credentials
            }

        override_entry = {
            "DataSourceId": src_id,
            "Name": (tgt or src).get("Name"),
            "DataSourceParameters": ds_params,
            "Credentials": credentials_block,
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
