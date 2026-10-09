import argparse
import base64
import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def run_json_command(args_list):
    """
    Runs a command and returns parsed JSON output.
    Raises RuntimeError on non-zero exit or invalid JSON.
    """
    # Mask credential blobs in printed command output
    safe_args = []
    skip_next = False
    for arg in args_list:
        if skip_next:
            safe_args.append("****")
            skip_next = False
        elif arg in ("--credentials", "--data-source-credentials"):
            safe_args.append(arg)
            skip_next = True
        else:
            safe_args.append(f'"{arg}"' if " " in arg else arg)
    print("Running:", " ".join(safe_args))

    process = subprocess.run(
        args_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if process.returncode != 0:
        raise RuntimeError(process.stderr.strip() or process.stdout.strip())

    try:
        return json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse JSON output: {exc}") from exc


def list_all_data_sources(account_id, region, no_verify_ssl=True):
    data_sources = []
    next_token = None

    while True:
        cmd = [
            "aws",
            "quicksight",
            "list-data-sources",
            "--aws-account-id",
            account_id,
            "--region",
            region,
            "--output",
            "json",
        ]

        if no_verify_ssl:
            cmd.append("--no-verify-ssl")

        if next_token:
            cmd.extend(["--next-token", next_token])

        payload = run_json_command(cmd)
        data_sources.extend(payload.get("DataSources", []))
        next_token = payload.get("NextToken")

        if not next_token:
            break

    return data_sources


def describe_data_source(account_id, region, data_source_id, no_verify_ssl=True):
    cmd = [
        "aws",
        "quicksight",
        "describe-data-source",
        "--aws-account-id",
        account_id,
        "--data-source-id",
        data_source_id,
        "--region",
        region,
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    payload = run_json_command(cmd)
    return payload.get("DataSource", {})


def extract_key_pair_credentials(secret_json, require_encrypted_key=False):
    username = (
        secret_json.get("username")
        or secret_json.get("key_pair_username")
        or secret_json.get("KeyPairUsername")
    )
    private_key = (
        secret_json.get("key")
        or secret_json.get("Key")
        or secret_json.get("private_key")
        or secret_json.get("privateKey")
        or secret_json.get("PrivateKey")
    )
    password = secret_json.get("password") or secret_json.get("Password")
    private_key_passphrase = (
        secret_json.get("keyPassphrase")
        or secret_json.get("KeyPassphrase")
        or secret_json.get("private_key_passphrase")
        or secret_json.get("privateKeyPassphrase")
        or secret_json.get("passphrase")
        or secret_json.get("PrivateKeyPassphrase")
    )

    if not username:
        raise RuntimeError(
            "SecretString JSON must include non-empty 'username' field."
        )

    if not private_key and password:
        raise RuntimeError(
            "Secret contains 'password' but no 'key'. "
            "This script updates KeyPair credentials only. Use a key-pair secret "
            "(for example datasource_secret_arn_keypair.json) with an encrypted PEM private key."
        )

    if not private_key:
        raise RuntimeError(
            "SecretString JSON must include non-empty private key field for KeyPair mode. "
            "Preferred field names are 'key' and 'keyPassphrase'."
        )

    if require_encrypted_key and not private_key_passphrase:
        raise RuntimeError(
            "Encrypted KeyPair mode requires non-empty 'keyPassphrase' in the secret JSON."
        )

    private_key_text = normalize_private_key_pem(private_key)
    if len(private_key_text) < 200 or len(private_key_text) > 20000:
        raise RuntimeError(
            "Private key must be a PEM key between 200 and 20000 characters after normalization. "
            "If your secret still stores a plain password, update it to an encrypted key pair private key in the key field."
        )

    return username, private_key_text, private_key_passphrase


def normalize_private_key_pem(private_key):
    key_text = str(private_key).strip()
    if not key_text:
        raise RuntimeError("Private key value is empty.")

    # Accept optional quoted key values copied from shell or JSON snippets.
    if len(key_text) >= 2 and key_text[0] == key_text[-1] and key_text[0] in ('"', "'"):
        key_text = key_text[1:-1].strip()

    # Support secrets where newline characters were escaped during storage.
    key_text = key_text.replace("\\r\\n", "\n").replace("\\n", "\n")
    key_text = key_text.replace("\r\n", "\n").replace("\r", "\n")

    # Extract the first PEM block if extra text is present around it.
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
            raise RuntimeError(
                "Private key PEM block has mismatched BEGIN/END labels."
            )

        if begin_label == "OPENSSH PRIVATE KEY":
            raise RuntimeError(
                "OPENSSH private keys are not supported for QuickSight KeyPair credentials. "
                "Convert to PKCS8 PEM (BEGIN PRIVATE KEY) and retry."
            )

        body_compact = "".join(body.split())
        if body_compact and not re.fullmatch(r"[A-Za-z0-9+/=]+", body_compact):
            raise RuntimeError(
                "Private key PEM body contains non-base64 characters."
            )

        normalized_body_lines = [
            body_compact[i : i + 64] for i in range(0, len(body_compact), 64)
        ]
        return begin_line + "\n" + "\n".join(normalized_body_lines) + "\n" + end_line + "\n"

    # Fallback: accept base64 body-only keys and wrap as PKCS8 PEM.
    compact = "".join(key_text.split())
    compact = compact.replace("-", "+").replace("_", "/")
    if not re.fullmatch(r"[A-Za-z0-9+/=]+", compact):
        raise RuntimeError(
            "Private key must be PEM formatted or base64 DER text. "
            "Expected BEGIN/END markers or base64 key content."
        )

    # Add required padding for raw base64 strings before decode validation.
    compact += "=" * ((4 - len(compact) % 4) % 4)
    try:
        base64.b64decode(compact, validate=True)
    except Exception as exc:
        raise RuntimeError(
            "Private key base64 content is invalid. Provide a valid PEM key or base64 DER text."
        ) from exc

    body_lines = [compact[i : i + 64] for i in range(0, len(compact), 64)]
    return "-----BEGIN PRIVATE KEY-----\n" + "\n".join(body_lines) + "\n-----END PRIVATE KEY-----\n"


def get_and_validate_secret(secret_arn, region, no_verify_ssl, out_dir):
    cmd = [
        "aws",
        "secretsmanager",
        "get-secret-value",
        "--secret-id",
        secret_arn,
        "--region",
        region,
        "--output",
        "json",
    ]

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    payload = run_json_command(cmd)
    secret_string = payload.get("SecretString")
    if not secret_string:
        raise RuntimeError("SecretString is empty. This script expects a JSON string secret.")

    try:
        secret_json = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"SecretString is not valid JSON: {exc}") from exc

    if not isinstance(secret_json, dict):
        raise RuntimeError("SecretString JSON must be an object with username/key/keyPassphrase fields.")

    extract_key_pair_credentials(secret_json)

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    secret_dump_path = out_dir / f"validated_secret_value_{ts}.json"
    secret_dump_path.write_text(json.dumps(secret_json, indent=2), encoding="utf-8")
    return secret_json, secret_dump_path
def filter_by_data_source_type(data_sources, data_source_types):
    if not data_source_types:
        return data_sources

    expected_types = {item.strip().upper() for item in data_source_types if item.strip()}
    return [
        source
        for source in data_sources
        if str(source.get("Type", "")).upper() in expected_types
    ]


def load_data_sources_from_file(file_path):
    path = Path(file_path)
    if not path.is_file():
        raise RuntimeError(f"DataSources file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse DataSources file JSON: {exc}") from exc

    items = payload.get("Items", [])
    if not isinstance(items, list):
        raise RuntimeError("Invalid DataSources file format: 'Items' must be a list")

    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        data_source_id = item.get("DataSourceId")
        if not data_source_id:
            continue
        normalized.append(
            {
                "DataSourceId": data_source_id,
                "Name": item.get("Name", ""),
                "Type": item.get("Type", ""),
                "Status": item.get("Status", ""),
            }
        )

    return payload, normalized


def load_secret_arn_from_file(file_path, account_id, region):
    path = Path(file_path)
    if not path.is_file():
        raise RuntimeError(f"Secret file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse secret file JSON: {exc}") from exc

    secret_arn = payload.get("secret_arn") or payload.get("secretArn") or payload.get("SecretArn")
    if secret_arn:
        return secret_arn

    secret_name = payload.get("secret_name") or payload.get("secretName") or payload.get("SecretName")
    if secret_name:
        return f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_name}"

    raise RuntimeError(
        "Secret file must include one of: secret_arn/secretArn/SecretArn "
        "or secret_name/secretName/SecretName"
    )


def _process_key_via_openssl(key_pem, passphrase=None):
    """
    Converts/decrypts any supported PEM private key to unencrypted PKCS#8 PEM.
    Handles: BEGIN ENCRYPTED PRIVATE KEY, BEGIN PRIVATE KEY, BEGIN RSA PRIVATE KEY.
    Tries the 'cryptography' package first, then falls back to openssl pkey subprocess.
    """
    # Attempt 1: cryptography library
    try:
        from cryptography.hazmat.primitives.serialization import (
            load_pem_private_key,
            Encoding,
            PrivateFormat,
            NoEncryption,
        )
        pw = passphrase.encode() if isinstance(passphrase, str) else passphrase
        key = load_pem_private_key(key_pem.encode(), password=pw)
        return key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        ).decode()
    except ImportError:
        pass  # fall through to openssl
    except Exception as exc:
        raise RuntimeError(f"Failed to process private key (cryptography): {exc}") from exc

    # Attempt 2: openssl pkey subprocess (passin via stdin to avoid shell exposure)
    # openssl pkey handles all key types and outputs unencrypted PKCS#8 by default.
    import os, tempfile
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
            f.write(key_pem)
            tmp_path = f.name
        cmd = ["openssl", "pkey", "-in", tmp_path]
        if passphrase:
            cmd += ["-passin", "stdin"]
        result = subprocess.run(
            cmd,
            input=passphrase or "",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"openssl pkey failed: {result.stderr.strip()}")
        return result.stdout
    except FileNotFoundError:
        raise RuntimeError(
            "Cannot process private key: 'cryptography' package is not installed "
            "and 'openssl' is not available. Run: pip install cryptography"
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def prepare_private_key_for_quicksight(private_key, passphrase=None):
    """
    Returns a normalized PEM string for QuickSight's PrivateKey field.

    QuickSight requires full PEM with headers and exactly 64-char body lines:
      -----BEGIN (ENCRYPTED )?PRIVATE KEY-----
      <64-char base64 lines>
      -----END (ENCRYPTED )?PRIVATE KEY-----

    Key types handled:
      - BEGIN ENCRYPTED PRIVATE KEY  (PKCS#8 encrypted)  -> pass as-is normalized; QuickSight
                                                             decrypts using PrivateKeyPassphrase
      - BEGIN PRIVATE KEY            (PKCS#8 unencrypted) -> normalize line wrapping
      - BEGIN RSA PRIVATE KEY        (PKCS#1)             -> convert to PKCS#8 via openssl, normalize
      - bare base64 (no headers)                          -> wrap as PKCS#8, normalize
    """
    key_text = str(private_key).strip()
    # Normalize any escaped newlines stored in JSON/secrets
    key_text = key_text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")

    is_rsa_pkcs1 = "BEGIN RSA PRIVATE KEY" in key_text
    is_rsa_encrypted = is_rsa_pkcs1 and "Proc-Type:" in key_text and "ENCRYPTED" in key_text

    if is_rsa_pkcs1:
        # PKCS#1 is not accepted by QuickSight — convert to PKCS#8 first
        if is_rsa_encrypted:
            if not passphrase:
                raise RuntimeError(
                    "Private key uses encrypted 'BEGIN RSA PRIVATE KEY' but no passphrase was found in the secret. "
                    "Add a 'keyPassphrase' field to the secret."
                )
            print("[INFO] Encrypted RSA (PKCS#1) key \u2014 converting to PKCS#8 for QuickSight.")
        else:
            print("[INFO] RSA (PKCS#1) key \u2014 converting to PKCS#8 for QuickSight.")
        key_text = _process_key_via_openssl(key_text, passphrase if is_rsa_encrypted else None)

    elif "BEGIN ENCRYPTED PRIVATE KEY" in key_text:
        if not passphrase:
            raise RuntimeError(
                "Private key uses 'BEGIN ENCRYPTED PRIVATE KEY' but no passphrase was found in the secret. "
                "Add a 'keyPassphrase' field to the secret."
            )
        print("[INFO] Encrypted PKCS#8 key detected \u2014 will be sent to QuickSight with passphrase.")

    # Return normalized PEM (headers + 64-char lines) — QuickSight's required format
    return normalize_private_key_pem(key_text)


def update_data_source_with_key_pair(
    account_id,
    region,
    data_source_id,
    data_source_name,
    username,
    private_key,
    data_source_parameters,
    no_verify_ssl=True,
    vpc_connection_properties=None,
    ssl_properties=None,
    private_key_passphrase=None,
):
    # QuickSight requires full PEM with headers and 64-char body lines.
    private_key_pem = prepare_private_key_for_quicksight(private_key, private_key_passphrase)

    credentials_payload = {
        "KeyPairCredentials": {
            "KeyPairUsername": username,
            "PrivateKey": private_key_pem,
        }
    }
    if private_key_passphrase:
        credentials_payload["KeyPairCredentials"]["PrivateKeyPassphrase"] = private_key_passphrase
    credentials = json.dumps(credentials_payload)

    cmd = [
        "aws",
        "quicksight",
        "update-data-source",
        "--aws-account-id",
        account_id,
        "--data-source-id",
        data_source_id,
        "--name",
        data_source_name,
        "--credentials",
        credentials,
        "--data-source-parameters",
        json.dumps(data_source_parameters),
        "--region",
        region,
        "--output",
        "json",
    ]

    if vpc_connection_properties:
        cmd.extend(["--vpc-connection-properties", json.dumps(vpc_connection_properties)])

    if ssl_properties:
        cmd.extend(["--ssl-properties", json.dumps(ssl_properties)])

    if no_verify_ssl:
        cmd.append("--no-verify-ssl")

    return run_json_command(cmd)


def wait_for_data_source_update(account_id, region, data_source_id, no_verify_ssl, poll_interval=5, max_wait=120):
    """
    Polls describe-data-source until the DataSourceStatus is a terminal state.
    Raises RuntimeError if the update failed or timed out.
    """
    terminal_ok = {"UPDATE_SUCCESSFUL", "CREATION_SUCCESSFUL"}
    terminal_fail = {"UPDATE_FAILED", "CREATION_FAILED", "DELETED"}
    deadline = time.time() + max_wait

    while time.time() < deadline:
        detail = describe_data_source(account_id, region, data_source_id, no_verify_ssl=no_verify_ssl)
        ds_status = detail.get("Status", "")
        if ds_status in terminal_ok:
            return ds_status
        if ds_status in terminal_fail:
            error_info = detail.get("ErrorInfo", {})
            msg = error_info.get("Message") or error_info.get("Type") or ds_status
            raise RuntimeError(f"DataSource update failed with status {ds_status}: {msg}")
        time.sleep(poll_interval)

    raise RuntimeError(
        f"DataSource update did not complete within {max_wait}s. "
        f"Last status: {ds_status}. Check the QuickSight console for details."
    )


def ensure_keypair_authentication_type(data_source_parameters):
    """
    QuickSight requires AuthenticationType=KEYPAIR for connectors that support
    key pair auth (for example Snowflake and Starburst). If the existing
    DataSourceParameters still carry PASSWORD, UpdateDataSource can fail with
    CredentialPair flow errors.
    """
    params = json.loads(json.dumps(data_source_parameters))
    changed = False

    if isinstance(params.get("SnowflakeParameters"), dict):
        if params["SnowflakeParameters"].get("AuthenticationType") != "KEYPAIR":
            params["SnowflakeParameters"]["AuthenticationType"] = "KEYPAIR"
            changed = True

    if isinstance(params.get("StarburstParameters"), dict):
        if params["StarburstParameters"].get("AuthenticationType") != "KEYPAIR":
            params["StarburstParameters"]["AuthenticationType"] = "KEYPAIR"
            changed = True

    return params, changed


def build_args():
    parser = argparse.ArgumentParser(
        description=(
            "Update QuickSight DataSource credentials using AuthorizationType KeyPair. "
            "Lists DataSources in the account/region, optionally filters by type or ID, "
            "then updates each matched DataSource to use a PEM private key from the provided secret."
        )
    )
    parser.add_argument("--account-id", default="975049925760", help="AWS account ID that owns the QuickSight assets.")
    parser.add_argument("--region", default="us-east-1", help="AWS region for QuickSight API calls.")
    parser.add_argument("--secret-arn", default=None, help="AWS Secrets Manager ARN containing the KeyPair secret payload.")
    parser.add_argument(
        "--secret-file",
        default=None,
        help=(
            "Optional JSON file that contains secret_arn or secret_name. "
            "If provided, this value is used to resolve Secret ARN."
        ),
    )
    parser.add_argument(
        "--skip-secret-precheck",
        action="store_true",
        help="Deprecated for KeyPair mode. Secret is always fetched to build KeyPairCredentials.",
    )
    parser.add_argument(
        "--data-sources-file",
        default=None,
        help=(
            "Optional JSON file generated by run_listDataSource.py. "
            "When provided, updates are based on the file's Items list instead of listing from AWS."
        ),
    )
    parser.add_argument(
        "--data-source-type",
        action="append",
        default=[],
        help=(
            "Optional DataSource type filter (for example REDSHIFT, AURORA_POSTGRESQL, MYSQL). "
            "Repeat the flag to include multiple types. Omit to target all DataSources."
        ),
    )
    parser.add_argument(
        "--data-source-id",
        action="append",
        default=[],
        help="Optional DataSource ID to target. Repeat the flag for multiple IDs.",
    )
    parser.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "downloads"), help="Directory where the results JSON file will be written.")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL certificate verification for AWS CLI calls.")
    parser.add_argument("--verify-ssl", action="store_true", help="Enable SSL certificate verification for AWS CLI calls.")
    parser.add_argument("--dry-run", action="store_true", help="List matched DataSources without applying updates.")
    parser.add_argument(
        "--env",
        default=None,
        help="Optional environment hint. When set to 'uat', encrypted key+passphrase is enforced.",
    )
    parser.add_argument(
        "--strict-encrypted-secret",
        action="store_true",
        help="Require encrypted key secret shape (username + key + keyPassphrase).",
    )
    return parser.parse_args()


def main():
    args = build_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    src_account_id = args.account_id
    src_region = args.region
    no_verify_ssl = True

    if args.verify_ssl:
        no_verify_ssl = False
    elif args.no_verify_ssl:
        no_verify_ssl = True

    secret_arn = args.secret_arn
    if args.secret_file:
        secret_arn = load_secret_arn_from_file(args.secret_file, src_account_id, src_region)
        print(f"Loaded Secret ARN from file: {args.secret_file}")

    if not secret_arn:
        print("[ERROR] Provide --secret-arn or --secret-file")
        sys.exit(1)

    if args.skip_secret_precheck:
        print("[WARN] --skip-secret-precheck is ignored in KeyPair mode. Secret is required to build KeyPairCredentials.")

    secret_json, secret_dump_path = get_and_validate_secret(secret_arn, src_region, no_verify_ssl, out_dir)
    print(f"[OK] Secret loaded for KeyPair auth and saved to: {secret_dump_path}")
    normalized_env = (args.env or "").strip().lower()
    require_encrypted_key = args.strict_encrypted_secret or normalized_env == "uat"
    if require_encrypted_key:
        print("[INFO] Encrypted key enforcement is enabled for this run.")
    username, private_key, private_key_passphrase = extract_key_pair_credentials(
        secret_json,
        require_encrypted_key=require_encrypted_key,
    )
    if private_key_passphrase:
        print("[OK] Passphrase found in secret — will be supplied during authentication.")
    else:
        print("[INFO] No passphrase found in secret — authenticating with private key only.")

    selected_types = [item.strip().upper() for item in args.data_source_type if item.strip()]
    selected_ids = {item.strip() for item in args.data_source_id if item.strip()}

    source_file_used = None
    if args.data_sources_file:
        print(f"Loading data sources from file: {args.data_sources_file}")
        payload, data_sources = load_data_sources_from_file(args.data_sources_file)
        source_file_used = str(Path(args.data_sources_file).resolve())
        print(f"Loaded {len(data_sources)} data source(s) from file.")

        file_account = payload.get("AwsAccountId")
        file_region = payload.get("Region")
        if file_account and str(file_account) != str(src_account_id):
            print(f"[WARN] account-id differs from file metadata: cli={src_account_id}, file={file_account}. Using CLI account-id.")
        if file_region and str(file_region) != str(src_region):
            print(f"[WARN] region differs from file metadata: cli={src_region}, file={file_region}. Using CLI region.")
    else:
        print(f"Listing data sources for account={src_account_id}, region={src_region}")
        data_sources = list_all_data_sources(src_account_id, src_region, no_verify_ssl=no_verify_ssl)
        print(f"Found {len(data_sources)} total data source(s).")

    if selected_types:
        data_sources = filter_by_data_source_type(data_sources, selected_types)
        print(f"Applied type filter: {', '.join(selected_types)} | Matched {len(data_sources)} data source(s).")

    if selected_ids:
        data_sources = [s for s in data_sources if s.get("DataSourceId", "") in selected_ids]
        print(f"Applied ID filter | Matched {len(data_sources)} data source(s).")

    if not data_sources:
        print("No DataSources matched. Nothing to update.")
        return

    if args.dry_run:
        print("[DRY RUN] The following DataSources would be updated with KeyPair credentials:")
        for source in data_sources:
            print(f"  {source.get('Name')} ({source.get('DataSourceId')}) | Type={source.get('Type')} | Status={source.get('Status')}")
        print(f"\nSecret ARN source: {secret_arn}")
        print(f"Username from secret: {username}")
        print(f"Passphrase: {'provided' if private_key_passphrase else 'not set'}")
        return

    results = []
    print(f"\nUpdating {len(data_sources)} data source(s) with AuthorizationType=KeyPair\n")

    for source in data_sources:
        data_source_id = source.get("DataSourceId", "")
        listed_name = source.get("Name", "")
        source_type = source.get("Type", "")

        try:
            detail = describe_data_source(src_account_id, src_region, data_source_id, no_verify_ssl=no_verify_ssl)
            current_name = detail.get("Name") or listed_name or data_source_id
            params = detail.get("DataSourceParameters")
            vpc_props = detail.get("VpcConnectionProperties")
            ssl_props = detail.get("SslProperties")

            if not params:
                raise RuntimeError("DescribeDataSource did not return DataSourceParameters")

            params, auth_type_changed = ensure_keypair_authentication_type(params)
            if auth_type_changed:
                print(f"[INFO] Set DataSourceParameters.AuthenticationType=KEYPAIR for: {current_name} ({data_source_id})")

            update_response = update_data_source_with_key_pair(
                src_account_id,
                src_region,
                data_source_id,
                current_name,
                username,
                private_key,
                params,
                no_verify_ssl=no_verify_ssl,
                vpc_connection_properties=vpc_props,
                ssl_properties=ssl_props,
                private_key_passphrase=private_key_passphrase,
            )
            immediate_status = update_response.get("UpdateStatus", "")
            if immediate_status == "UPDATE_FAILED":
                raise RuntimeError(f"update-data-source returned UPDATE_FAILED immediately for {data_source_id}")
            final_status = wait_for_data_source_update(
                src_account_id, src_region, data_source_id, no_verify_ssl=no_verify_ssl
            )
            status = "SUCCESS"
            error = None
            passphrase_note = " (with passphrase)" if private_key_passphrase else ""
            print(f"[OK] Updated credentials for: {current_name} ({data_source_id}){passphrase_note} | final status: {final_status}")
        except RuntimeError as exc:
            status = "FAILED"
            error = str(exc)
            print(f"[FAIL] {listed_name} ({data_source_id}): {error}")

        results.append(
            {
                "DataSourceId": data_source_id,
                "Name": listed_name,
                "Type": source_type,
                "UpdateStatus": status,
                "Error": error,
            }
        )

    success_count = sum(1 for r in results if r["UpdateStatus"] == "SUCCESS")
    fail_count = len(results) - success_count

    output_payload = {
        "AwsAccountId": src_account_id,
        "Region": src_region,
        "AuthType": "key_pair",
        "SecretArn": secret_arn,
        "KeyPairUsername": username,
        "Username": username,
        "PassphraseProvided": bool(private_key_passphrase),
        "SecretFile": str(Path(args.secret_file).resolve()) if args.secret_file else None,
        "ValidatedSecretFile": str(secret_dump_path) if secret_dump_path else None,
        "DataSourcesFile": source_file_used,
        "DataSourceTypeFilter": selected_types,
        "TotalMatched": len(results),
        "SuccessCount": success_count,
        "FailCount": fail_count,
        "Items": results,
    }

    out_file = out_dir / f"update_datasource_key_pair_{src_account_id}_{src_region}.json"
    out_file.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print(f"\n[OK] Saved results to: {out_file}")
    print(f"Updated: {success_count} | Failed: {fail_count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)



