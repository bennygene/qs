import subprocess
import time
import re
 
def run_command_and_wait(cmd):
    print(f"Running: {cmd}")
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    download_url_found = False
 
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            # Check for the success message
            if re.search(r'Download URL: SUCCESSFUL', output):
                download_url_found = True
                break
 
    # Wait for process to finish if not already
    process.wait()
    return download_url_found
 
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 937dcbdc-a798-4509-83db-950725657a9c --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 6de95843-be2d-41ec-a110-0a3059812249 --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id b89af02e-e087-44fc-9858-66115623334b --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 87e4351c-9fcb-48cd-a52a-bc310ed10016 --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 30b350e0-5b73-4a44-a01a-7c5d2a8a8df1 --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 405243ea-1e7c-455d-8170-1c4f809c5d89 --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id b97ff866-c165-407f-a452-05c70992821b --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 3727d7a5-8894-4703-bdd8-c2bba6f05c56 --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id a128ac3a-7388-4d54-ac78-d65a00de74de --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 699e1c5f-4fd8-4e6f-9c73-1d5d1c34429f --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id 60816c99-edf3-4a46-9811-455bcbaf2588 --folder-path ./quicksight/downloads',
      #  'python quicksight/export_dashboard_v1.py --account-id 034362059803 --region ap-south-1 --dashboard-id ca7ed5fb-b459-4c15-8463-b3f6c28f529c --folder-path ./quicksight/downloads'
 
def main():
    # List of commands to run
    commands = [
#NLP De Rule Definition Master
#'python exportDashboardNEW.py --account-id 650251710248 --region ap-south-1 --resource-arn arn:aws:quicksight:ap-south-1:650251710248:dataset/ff9dcb7f-e028-49b5-af39-b9b310b38356 --folder-path ./quicksight/downloads',
#MRD Topic NLP
#'python exportDashboardNEW.py --account-id 650251710248 --region ap-south-1 --resource-arn arn:aws:quicksight:ap-south-1:650251710248:dataset/fbbeb1e4-395c-4be4-af17-a2b8ab7facc8 --folder-path ./quicksight/downloads'
#--NLP Topics - NOT YET SUPPORTED by StartAssetBundleExportJob
#'python exportDashboardNEW.py --account-id 650251710248 --region ap-south-1 --resource-arn arn:aws:quicksight:ap-south-1:650251710248:topic/fN5StmAmNarqCTTh0EzUX8geWWVQeZ9P --folder-path ./quicksight/downloads'
] 
 
    for cmd in commands:
        success = run_command_and_wait(cmd)
        if not success:
            print("Did not find 'Download URL: SUCCESSFUL' message. Stopping batch.")
            break
        time.sleep(2)  # Optional: wait a bit before next command
 
if __name__ == "__main__":
    main()
