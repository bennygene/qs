import os
import subprocess
import json
import configparser
import argparse
import sys
import logging
from datetime import datetime

def run_command(command, step_description, logger):
    try:
        logger.info(f"Running: {step_description}")
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        output_lines = []
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                line = output.strip()
                output_lines.append(line)
                logger.info(line)

        return_code = process.poll()
        if return_code != 0:
            logger.error(f"Command failed with exit code {return_code}")
            return False, output_lines
        return True, output_lines
    except Exception as e:
        logger.error(f"Exception during {step_description}: {e}")
        return False, []

def setup_logging():
    log_filename = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_filename)
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def main():
    parser = argparse.ArgumentParser(description="Migrate QuickSight datasets across environments.")
    parser.add_argument("--target-config", required=True, help="JSON string or path to JSON file with environment config.")
    parser.add_argument("--target-name", required=True, help="Key name in the JSON config (e.g., emea-dev)")
    args = parser.parse_args()

    logger = setup_logging()

    with open("datasets.json", "r") as f:
        datasets = json.load(f)

    config = configparser.ConfigParser()
    config.read("property.cfg")

    if os.path.isfile(args.target_config):
        with open(args.target_config, "r") as f:
            full_config = json.load(f)
    else:
        full_config = json.loads(args.target_config)

    if args.target_name not in full_config:
        logger.error(f"Target name '{args.target_name}' not found in config.")
        sys.exit(1)

    env_config = full_config[args.target_name]
    env = env_config["env"]
    # env = "cat"
    source_profile = env_config["source_profile"]
    target_profile = env_config["target_profile"]
    source_region = env_config["source_region"]
    target_region = env_config["target_region"]
    source_account_id = env_config["source_account_id"]
    target_account_id = env_config["target_account_id"]

    override_file = f"{env}_override_{source_account_id}_{source_region}_to_{target_account_id}_{target_region}.json"

    override_cmd = (
        f"python generateOverride.py "
        f"--env {env} "
        f"--prop-file property.cfg "
        f"--source-profile {source_profile} "
        f"--target-profile {target_profile} "
        f"--source-region {source_region} "
        f"--target-region {target_region} "
        f"--source-account-id {source_account_id} "
        f"--target-account-id {target_account_id} "
        f"--output {override_file}"
    )
    success, _ = run_command(override_cmd, "Generating override file", logger)
    if not success:
        sys.exit(1)

    for dataset in datasets:
        dataset_id = dataset["DatasetID"]
        dataset_name = dataset["Dataset"].replace(" ", "_")
        zip_path = f"./quicksight/downloads/{dataset_name}.zip"

        if dataset.get("Status") == "Success":
            logger.info(f"Skipping {dataset_name} (already processed successfully).")
            continue

        success = True

        export_cmd = (
            f"python exportDataSet.py "
            f"--account-id {source_account_id} "
            f"--region {source_region} "
            f"--dataset-id {dataset_id} "
            f"--folder-path ./quicksight/downloads"
        )
        success, _ = run_command(export_cmd, f"Exporting dataset: {dataset_name}", logger)

        if success:
            clean_cmd = f"python cleanZip.py {zip_path} --env {env}"
            success, _ = run_command(clean_cmd, f"Cleaning ZIP for dataset: {dataset_name}", logger)

        if success:
            import_cmd = (
                f"python importDataSet.py "
                f"--account-id {target_account_id} "
                f"--region {target_region} "
                f"--asset-bundle {zip_path} "
                f"--override {override_file} "
                f"--job-id {dataset_name} "
                f"--dataset-id {dataset_id} "
                f"--profile {target_profile}"
            )
            success, import_output = run_command(import_cmd, f"Importing dataset: {dataset_name}", logger)

            for line in import_output:
                if any(status in line.upper() for status in [
                    "FAILED_ROLLBACK_ERROR", "FAILED_ROLLBACK_IN_PROGRESS", "FAILED_ROLLBACK_COMPLETED"
                ]):
                    logger.error(f"Detected rollback failure for {dataset_name}: {line}")
                    success = False
                    dataset["Status"] = "FAILED_ROLLBACK_ERROR"
                    break

        if success:
            dataset["Status"] = "Success"
        elif dataset.get("Status") != "FAILED_ROLLBACK_ERROR":
            dataset["Status"] = "Failed"

        # Save progress after each dataset
        with open("datasets.json", "w") as f:
            json.dump(datasets, f, indent=2)

    logger.info("\DATASET MIGRATION SUMMARY")
    failed_datasets = [d for d in datasets if d["Status"] != "Success"]
    if failed_datasets:
        logger.info("Some datasets failed to process:")
        for d in failed_datasets:
            logger.info(f"  - S.No. {d['S.No.']}: {d['Dataset']} (ID: {d['DatasetID']})")
    else:
        logger.info("All datasets processed successfully with no failures.")

    logger.info("All datasets processed. Final statuses written to datasets.json.")

if __name__ == "__main__":
    main()

