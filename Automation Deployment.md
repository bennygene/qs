Below is the content converted to **Confluence Cloud (New Editor) format**.  
This uses **native Cloud Markdown-style formatting** (headings, bullets, and fenced code blocks) and can be pasted **directly into a Confluence Cloud page**.

***

# Automation Rollout Configuration

This page documents the steps and scripts required to download, clean, and import datasets across AWS environments using Python automation.

***

## Table of Contents

*   Prerequisites
*   Initial Environment Setup
*   Automation Rollout
*   Creating Override Files
*   Notes & Validation

***

## Prerequisites

Ensure the following are available before starting:

*   Python 3.x installed
*   Git Bash (recommended for Windows users)
*   Visual Studio Code (VSC)
*   Access to:
    *   Nexus PyPI proxy
    *   Britive (for AWS credentials)
    *   Source and target AWS accounts

***

## Initial Environment Setup

1.  Create a local working folder.

2.  Open **Visual Studio Code (VSC)**.

3.  Navigate to:

<!---->

    File → Open Folder

4.  Create a Python virtual environment:

```bash
python -m venv .venv
```

5.  Activate the virtual environment:

```bash
source .venv/Scripts/activate
```

6.  Configure `pip` to trust the Nexus host:

```bash
python -m pip config set global.trusted-host nexus-dev.onefiserv.net
```

7.  Install required Python dependencies:

**Install boto3**

```bash
python -m pip install -i https://nexus-dev.onefiserv.net/repository/pypi-proxy/simple boto3 --no-user
```

**Install requests**

```bash
python -m pip install -i https://nexus-dev.onefiserv.net/repository/pypi-proxy/simple requests --no-user
```

***

## Automation Rollout

### Execution Steps

1.  Activate the virtual environment (Git Bash recommended):

```bash
source .venv/Scripts/activate
```

2.  Collect the **Dataset ID** and populate the required fields in:

<!---->

    run_download.json

3.  Update **source**, **target**, and **override** configuration values in:

<!---->

    runjob.json

4.  Update the **AWS Keys credentials** text file retrieved from **Britive**.

5.  Download dataset `.zip` files:

```bash
python run_download.py
```

6.  Replace source data references with target environment values:

```bash
# --env values: dev | cat | prod
python runjob.py --process clean --env dev --input-folder downloads/
```

7.  Import datasets into the target account/server:

```bash
python runjob.py --process import
```

***

## Creating Override Files

### Steps

1.  Collect and update the **AWS Keys credentials** text file from **Britive**.

2.  Identify:
    *   Source AWS account ID
    *   Target AWS account ID
    *   Source AWS region
    *   Target AWS region

3.  Generate the override file:

```bash
python generateOverride.py \
  --env dev \
  --prop-file json \
  --source-profile source \
  --target-profile target \
  --source-region us-east-1 \
  --target-region us-east-1 \
  --source-account-id 559050241748 \
  --target-account-id 841162682218
```

4.  Verify and validate the generated JSON files.

***

## Notes & Validation

Ensure the following fields in the override JSON files meet the requirements below:

### Host

*   Must be lowercase
*   Example:

<!---->

    fiservgbsdev.us-west-2.privatelink.snowflakecomputing.com

### Database

Examples:

*   `CRANALYTICSDEV`
*   `CRANALYTICSCAT`
*   `CRANALYTICS`

### Warehouse

Examples:

*   `CRANALYTICS_WH`
*   `CRANALYTICS_CERT_WH`

### Username

Example:

    CRAPP_UI

### Password

*   Environment-specific value

### VpcConnection

*   Value sourced from the **QS Account VPC Connection**

***

## Support

For credential access, dataset issues, or environment configuration questions, contact your internal AWS, Britive, or platform support teams.

***

If you want next steps, I can:

*   Add **expand/collapse sections** (Confluence macros)
*   Insert **warning/info panels**
*   Create a **Runbook-style layout**
*   Split this into **child pages** for cleaner navigation

Just tell me how your Confluence space is organized.
