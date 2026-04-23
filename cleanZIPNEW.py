#!/usr/bin/env python3
import os
import json
import zipfile
import tempfile
import shutil
import re
import argparse
 
def remove_staticfiles_folder(root_dir):
    """
    Recursively remove any folder named 'staticFiles' (case-insensitive) from the directory tree.
    """
    for foldername, dirnames, _ in os.walk(root_dir):
        # Make a copy of dirnames to avoid modifying the list while iterating
        for dirname in list(dirnames):
            if dirname.lower() == "staticfiles":
                folder_path = os.path.join(foldername, dirname)
                print(f"Removing staticFiles folder: {folder_path}")
                shutil.rmtree(folder_path)
                dirnames.remove(dirname)  # Prevent further walk into this directory
 
def find_all_static_file_ids(obj):
    ids = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if "staticfileid" in k.lower() and isinstance(v, str):
                ids.add(v)
            else:
                ids.update(find_all_static_file_ids(v))
    elif isinstance(obj, list):
        for item in obj:
            ids.update(find_all_static_file_ids(item))
    return ids
 
def remove_unused_static_files(bundle_json):
    if not isinstance(bundle_json, dict):
        return bundle_json
    used_static_file_ids = find_all_static_file_ids(bundle_json)
    if "staticFiles" in bundle_json:
        print(f"Found staticFiles with {len(bundle_json['staticFiles'])} entries.")
        bundle_json["staticFiles"] = [
            sf for sf in bundle_json["staticFiles"]
            if sf.get("staticFileId") in used_static_file_ids
        ]
        print(f"After cleaning, {len(bundle_json['staticFiles'])} staticFiles remain.")
    return bundle_json
 
def remove_null_principals(data):
    """
    Recursively remove any dict/list entries where 'principal' is None or missing.
    """
    if isinstance(data, dict):
        # If this dict has a 'principal' key and it's None, return None to signal removal
        if 'principal' in data and (data['principal'] is None):
            return None
        # Otherwise, recursively clean all values
        return {k: v for k, v in ((k, remove_null_principals(v)) for k, v in data.items()) if v is not None}
    elif isinstance(data, list):
        # Remove any dict in the list where 'principal' is missing or None
        cleaned = []
        for item in data:
            cleaned_item = remove_null_principals(item)
            if isinstance(cleaned_item, dict):
                if 'principal' in cleaned_item and cleaned_item['principal'] is None:
                    continue
                if 'principal' not in cleaned_item and any(k in cleaned_item for k in ['Actions', 'actions', 'Principal', 'principal']):
                    # If it's a permission-like object but missing principal, skip it
                    continue
            if cleaned_item is not None:
                cleaned.append(cleaned_item)
        return cleaned
    return data
 
def remove_invalid_principals(data, valid_account_id):
    """
    Recursively remove any dict entry where key is 'principal' and value is an ARN not matching valid_account_id.
    """
    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            if k.lower() == "principal" and isinstance(v, str):
                # Check if ARN contains a different account ID
                m = re.match(r"arn:aws:quicksight:[^:]+:(\d+):", v)
                if m and m.group(1) != valid_account_id:
                    print(f"Removing invalid principal: {v}")
                    continue  # Skip this key-value pair
            new_dict[k] = remove_invalid_principals(v, valid_account_id)
        return new_dict
    elif isinstance(data, list):
        return [remove_invalid_principals(item, valid_account_id) for item in data]
    return data
 
def fix_fieldwell_sort_consistency(analysis_json):
    print("Entered fix_fieldwell_sort_consistency")
    print("Top-level keys:", list(analysis_json.keys()))
    if not isinstance(analysis_json, dict):
        print("analysis_json is not a dict!")
        return
 
    definition = analysis_json.get("definition", {})
    sheets = definition.get("Sheets") or definition.get("sheets") or []
    print("Sheets found:", len(sheets))
    for sheet in sheets:
        visuals = sheet.get("Visuals") or sheet.get("visuals") or []
        print("  Visuals found in sheet:", len(visuals))
        for visual in visuals:
            visual_id = visual.get("VisualId") or visual.get("visualId") or "<no id>"
            print(f"  Checking visual: {visual_id}")
            # Collect all field IDs referenced in sort/order
            sort_fields = set()
            sort_config = visual.get("SortConfiguration", {})
            for opt in sort_config.get("FieldSortOptions", []):
                field_id = opt.get("FieldId")
                if field_id:
                    sort_fields.add(field_id)
            for opt in sort_config.get("CategorySort", []):
                field_id = opt.get("FieldId")
                if field_id:
                    sort_fields.add(field_id)
            for opt in sort_config.get("NumericSort", []):
                field_id = opt.get("FieldId")
                if field_id:
                    sort_fields.add(field_id)
 
            # For Table visuals, look for TableAggregatedFieldWells
            fieldwells = visual.get("FieldWells", {})
            table_wells = fieldwells.get("TableAggregatedFieldWells")
            if table_wells:
                groupby = table_wells.get("GroupBy", [])
                values = table_wells.get("Values", [])
                fieldwell_fields = set()
                for item in groupby + values:
                    if isinstance(item, dict):
                        fid = item.get("FieldId")
                        if fid:
                            fieldwell_fields.add(fid)
                print(f"    Sort fields: {sort_fields}")
                print(f"    FieldWell fields before fix: {fieldwell_fields}")
                missing = sort_fields - fieldwell_fields
                if missing:
                    print(f"    Visual {visual_id}: Missing fields in FieldWell: {missing}")
                # Add missing fields to GroupBy and Values as dicts if not present
                for m in missing:
                    if not any(d.get("FieldId") == m for d in groupby):
                        print(f"      Adding {m} to GroupBy")
                        groupby.append({"FieldId": m})
                    if not any(d.get("FieldId") == m for d in values):
                        print(f"      Adding {m} to Values")
                        values.append({"FieldId": m})
                table_wells["GroupBy"] = groupby
                table_wells["Values"] = values
                fieldwells["TableAggregatedFieldWells"] = table_wells
                visual["FieldWells"] = fieldwells
                print(f"    FieldWell fields after fix: {[d.get('FieldId') for d in groupby + values if isinstance(d, dict)]}")
            else:
                if sort_fields:
                    print(f"    Visual {visual_id}: TableAggregatedFieldWells not found, sort_fields: {sort_fields}")
 
def fix_field_options_order_consistency(analysis_json):
    """
    For each visual, ensure that any fieldId in fieldOptions.order is present in the FieldWell.
    If not, remove it from the order list.
    """
    if not isinstance(analysis_json, dict):
        return
 
    definition = analysis_json.get("definition", {})
    sheets = definition.get("Sheets") or definition.get("sheets") or []
    for sheet in sheets:
        visuals = sheet.get("Visuals") or sheet.get("visuals") or []
        for visual in visuals:
            # Collect all field IDs in FieldWells
            fieldwells = visual.get("FieldWells", {})
            fieldwell_fields = set()
            def collect_fields(obj):
                if isinstance(obj, dict):
                    for v in obj.values():
                        collect_fields(v)
                elif isinstance(obj, list):
                    for item in obj:
                        collect_fields(item)
                elif isinstance(obj, str):
                    fieldwell_fields.add(obj)
            collect_fields(fieldwells)
 
            # Remove from fieldOptions.order any fieldId not in FieldWells
            chart_config = visual.get("ChartConfiguration", {})
            field_options = chart_config.get("FieldOptions", {})
            order = field_options.get("Order")
            if isinstance(order, list):
                new_order = [fid for fid in order if fid in fieldwell_fields]
                if len(new_order) != len(order):
                    print(f"    Visual {visual.get('VisualId', '<no id>')}: Removing orphaned fields from order: {set(order) - fieldwell_fields}")
                    field_options["Order"] = new_order
 
def remove_vpcconnection_folder(root_dir):
    for folder in os.listdir(root_dir):
        folder_path = os.path.join(root_dir, folder)
        if os.path.isdir(folder_path) and folder.lower() == "vpcconnection":
            print(f"Removing folder: {folder_path}")
            shutil.rmtree(folder_path)
 
def fix_id(id_str):
    # Replace any character not allowed with underscore
    return re.sub(r'[^A-Za-z0-9\-_]', '_', id_str)
 
def fix_ids(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            # Fix keys ending with Id or Ids (case-insensitive)
            if (k.lower().endswith('id') or k.lower().endswith('ids')) and isinstance(v, str):
                obj[k] = fix_id(v)
            else:
                fix_ids(v)
    elif isinstance(obj, list):
        for item in obj:
            fix_ids(item)
 
def remove_keys(data):
    """
    Recursively remove any key called "permission" or "permissions" (case insensitive)
    from the JSON data.
    """
    if isinstance(data, dict):
        return { k: remove_keys(v)
                 for k, v in data.items()
                 if k.lower() not in ("permission", "permissions") }
    elif isinstance(data, list):
        return [remove_keys(item) for item in data]
    return data
 
def update_dataset_schema_with_env(data, env):
    env_lower = env.lower()
   
    # Compile a regex that matches any token from the three groups.
    pattern = re.compile(
        r'\b(CRANALYTICSDEV|CRANALYTICSCAT|CRANALYTICS|'
        r'CRRISKDBDEV|CRRISKCATDB|CRRISKDEVDB|CRRISKDB|'
        r'CRDATAHUBDEV|CRDATAHUB|CRCOMMRISKDB)\b',
        flags=re.IGNORECASE
    )
   
    def replacement(match):
        token = match.group(0).upper()
        # Group 1: CRANALYTICS group.
        if token in {"CRANALYTICS", "CRANALYTICSCAT", "CRANALYTICSDEV"}:
            if env_lower in ["qa", "dev"]:
                return "CRANALYTICSDEV"
            elif env_lower == "cat":
                return "CRANALYTICSCAT"
            elif env_lower == "prod":
                return "CRANALYTICS"
            else:
                return token
        # Group 2: CRRISK group.
        elif token in {"CRRISKDB", "CRRISKCATDB", "CRRISKDEVDB", "CRRISKDBDEV"}:
            if env_lower in ["qa", "dev"]:
                if token in {"CRRISKDB", "CRRISKCATDB"}:
                    return "CRRISKDEVDB"
                else:
                    return token
            elif env_lower == "cat":
                if token in {"CRRISKDB", "CRRISKDEVDB"}:
                    return "CRRISKCATDB"
                else:
                    return token
            elif env_lower == "prod":
                if token in {"CRRISKDEVDB", "CRRISKQADB","CRRISKCATDB"}:
                    return "CRRISKDB"
                else:
                    return token
            else:
                return token
        # Group 3: CRDATAHUB group.
        elif token in {"CRDATAHUB", "CRDATAHUBDEV","CRDATAHUBCAT"}:
            if env_lower in ["qa", "dev"]:
                return "CRDATAHUBDEV"
            elif env_lower == "cat":
                return "CRDATAHUBCAT"
            elif env_lower == "prod":
                return "CRDATAHUB"
            else:
                return token
        elif token in {"CRCOMMRISKDB"}:
            if env_lower == "cat":
                return "CRCOMMRISKCATDB"
        elif env_lower == "cat":
            if token == "CRCOMMRISKDB":
                return "CRCOMMRISKCATDB"
            elif token in {"CRANALYTICS", "CRANALYTICSCAT", "CRANALYTICSDEV"}:
                return "CRANALYTICSCAT"
            elif token in {"CRRISKDB", "CRRISKDEVDB"}:
                return "CRRISKCATDB"
            elif token in {"CRDATAHUB", "CRDATAHUBDEV"}:
                return "CRDATAHUBCAT"
            else:
                return token
        else:
            return token
 
    def recursive_update(obj):
        if isinstance(obj, dict):
            new_obj = {}
            for k, v in obj.items():
                # If the key is either "query" or "sqlQuery", process it.
                if isinstance(v, str) and k.lower() in ("query", "sqlquery"):
                    new_obj[k] = pattern.sub(replacement, v)
                else:
                    new_obj[k] = recursive_update(v)
            return new_obj
        elif isinstance(obj, list):
            return [recursive_update(item) for item in obj]
        else:
            return obj
 
    return recursive_update(data)
 
def process_json_files(root_folder, env=None):
    for folder, _, files in os.walk(root_folder):
        is_analysis = os.path.basename(folder).lower() == "analysis"
        for file in files:
            if file.lower().endswith(".json"):
                filepath = os.path.join(folder, file)
                if is_analysis:
                    print(f"\n[Analysis] Checking FieldWell/Sort and FieldOptions/Order consistency in {filepath}")
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            # Remove invalid principals
                        data = remove_invalid_principals(data, "277707139878")
                        data = remove_null_principals(data)
                        data = remove_unused_static_files(data)
                        #fix_fieldwell_sort_consistency(data)
                        #fix_field_options_order_consistency(data)
                        with open(filepath, "w", encoding="utf-8") as f:
                            json.dump(data, f, indent=2)
                        print(f"  Finished fixing {filepath}")
                    except Exception as e:
                        print(f"Error fixing FieldWell/Sort/Order in {filepath}: {e}")
                else:
                    print(f"\n[Other] Processing {filepath}")
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        # Remove permission-related keys.
                        cleaned_data = remove_keys(data)
                        cleaned_data = remove_unused_static_files(cleaned_data)
                        # Update query schema if this JSON is in a dataset folder.
                        if env:
                            cleaned_data = update_dataset_schema_with_env(cleaned_data, env)
                        # Fix invalid IDs
                        #fix_ids(cleaned_data)
                        with open(filepath, "w", encoding="utf-8") as f:
                            json.dump(cleaned_data, f, indent=2)
                        print(f"  Finished cleaning {filepath}")
                    except Exception as e:
                        print(f"Error processing {filepath}: {e}")
           
def rezip_directory(source_dir, output_zip):
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for folder, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(folder, file)
                # Preserve the directory structure.
                rel_path = os.path.relpath(file_path, source_dir)
                zf.write(file_path, rel_path)
 
def main():
    parser = argparse.ArgumentParser(
        description="Clean an asset bundle zip: remove permission keys and update dataset query schema."
    )
    parser.add_argument("zipfile", help="Path to the asset bundle zip file to process.")
    parser.add_argument("--env", type=str, choices=["prod", "qa", "cat", "dev"], required=True,
                        help="Environment to use (prod, qa, cat, or dev) for schema replacement.")
    parser.add_argument("--output", type=str,
                        help="(Optional) Path to the output cleaned zip file. Defaults to overwriting the input zip.")
    args = parser.parse_args()
 
    zip_filename = args.zipfile
    output_zip = args.output if args.output else zip_filename
 
    # Create a temporary directory for processing.
    temp_dir = tempfile.mkdtemp()
    print(f"Extracting {zip_filename} to temporary directory: {temp_dir}")
 
    try:
        # Extract the zip file.
        with zipfile.ZipFile(zip_filename, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
       
        # Remove vpcconnection folder if present
        remove_vpcconnection_folder(temp_dir)
        print(f"Extracted contents to: {temp_dir}")
 
        # Remove staticFiles folders
        remove_staticfiles_folder(temp_dir)
        print(f"Extracted contents to: {temp_dir}")
 
        # Process JSON files except those in analysis folder
        process_json_files(temp_dir, env=args.env)
 
        # Re-zip the processed contents.
        temp_zip = os.path.join(temp_dir, "temp_zip.zip")
        rezip_directory(temp_dir, temp_zip)
        print(f"Created cleaned zip file at: {temp_zip}")
       
        # Move the temporary zip to the final output location.
        shutil.move(temp_zip, output_zip)
        print(f"Replaced the original zip (or output new file) with: {output_zip}")
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        # Clean up temporary directory.
        shutil.rmtree(temp_dir)
        print(f"Removed temporary directory: {temp_dir}")
 
if __name__ == "__main__":
    main()
