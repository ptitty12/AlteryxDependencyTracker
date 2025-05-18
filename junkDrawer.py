#Pull workflows from Server

import requests
from requests.auth import HTTPBasicAuth
import warnings
from urllib3.exceptions import InsecureRequestWarning
import zipfile
import os
import sys
from typing import List, Optional

# --- Configuration ---
CLIENT_ID: str = "YOUR_CLIENT_ID_HERE"  # Replace with your Alteryx Gallery Client ID
CLIENT_SECRET: str = "YOUR_CLIENT_SECRET_HERE"  # Replace with your Alteryx Gallery Client Secret
BASE_URL: str = "YOUR_BASE_URL_HERE" 

# --- Global Settings ---
# Suppress only the specific InsecureRequestWarning for unverified HTTPS requests
warnings.simplefilter('ignore', InsecureRequestWarning)

# --- Functions ---

def extract_zip(zip_path: str, output_path: str) -> None:
    """
    Extract a zip file to the specified output path and then remove the zip file.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(output_path)
        print(f"Successfully extracted '{zip_path}' to '{output_path}'")
    except zipfile.BadZipFile:
        print(f"Error: '{zip_path}' is not a valid zip file or is corrupted.")
        raise
    except Exception as e:
        print(f"An error occurred during zip extraction: {e}")
        raise
    finally:
        if os.path.exists(zip_path):
            try:
                os.remove(zip_path)
                print(f"Removed zip file: '{zip_path}'")
            except OSError as e:
                print(f"Error removing zip file '{zip_path}': {e}")


def get_access_token(client_id: str, client_secret: str, token_base_url: str) -> str:
    """
    Get access token using OAuth2 client credentials grant.
    """
    token_url = f"{token_base_url}/oauth2/token"
    try:
        response = requests.post(
            token_url,
            auth=HTTPBasicAuth(client_id, client_secret),
            data={'grant_type': 'client_credentials'},
            verify=False
        )
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.HTTPError as e:
        error_message = f"Failed to get access token. Status Code: {e.response.status_code}. Response: {e.response.text}"
        if e.response.status_code == 401:
            error_message = "Failed to get access token: Unauthorized. Check Client ID and Secret."
        elif e.response.status_code == 403:
            error_message = "Failed to get access token: Forbidden. Check permissions."
        raise Exception(error_message) from e
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to get access token due to a network or request issue: {e}") from e
    except KeyError:
        raise Exception("Failed to get access token: 'access_token' not found in the response.")


def get_all_workflow_ids(access_token: str, api_base_url: str, view: Optional[str] = "Default") -> List[str]:
    """
    Retrieve all workflow IDs from the Alteryx server, handling pagination.
    The /v3/workflows endpoint uses limit/offset for pagination.
    Default limit is 20, Max is 100.
    """
    all_workflow_ids: List[str] = []
    workflows_url = f"{api_base_url}/v3/workflows"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    limit = 100  # Use the max allowed limit for fewer requests
    offset = 0
    print(f"Starting to fetch all workflow IDs from {workflows_url}...")

    while True:
        params = {
            'limit': limit,
            'offset': offset
        }
        if view:
            params['view'] = view

        try:
            # print(f"Fetching workflows with offset: {offset}, limit: {limit}")
            response = requests.get(workflows_url, headers=headers, params=params, verify=False)
            response.raise_for_status()
            workflows_batch = response.json()

            if not isinstance(workflows_batch, list):
                print(f"Warning: Unexpected response format at offset {offset}. Expected a list, got {type(workflows_batch)}.")
                break

            if not workflows_batch:
                # print("No more workflows returned in this batch.")
                break

            current_batch_ids = [wf.get("id") for wf in workflows_batch if wf.get("id")]
            all_workflow_ids.extend(current_batch_ids)
            # print(f"Fetched {len(current_batch_ids)} IDs in this batch. Total IDs so far: {len(all_workflow_ids)}.")

            if len(workflows_batch) < limit:
                # print("Last page of workflows reached.")
                break

            offset += len(workflows_batch) # More robust offset update based on items received

        except requests.exceptions.HTTPError as e:
            error_message = f"HTTP Error fetching workflows (offset {offset}): {e.response.status_code}."
            try:
                error_message += f" Response: {e.response.text}"
            except Exception:
                pass # Ignore if response text is not available
            if e.response.status_code == 401:
                error_message = "Unauthorized (401): Could not fetch workflows. The access token might be invalid or expired."
            raise Exception(error_message) from e
        except requests.exceptions.RequestException as e:
            raise Exception(f"Network error fetching workflows (offset {offset}): {e}") from e
        except ValueError as e: # For JSON decoding errors
            raise Exception(f"Error parsing workflow data (offset {offset}): {e}") from e
            
    print(f"Finished fetching. Total workflow IDs found: {len(all_workflow_ids)}")
    return all_workflow_ids


def download_workflow_package(
    workflow_id: str,
    access_token: str,
    api_base_url: str,
    temp_dir: str,
    version_id: Optional[str] = None
) -> str:
    """
    Download a workflow package from Alteryx and save it to a temporary directory.
    """
    os.makedirs(temp_dir, exist_ok=True)

    url = f"{api_base_url}/v3/workflows/{workflow_id}/package"
    if version_id:
        url = f"{url}?version={version_id}"

    headers = {
        'Accept': 'application/octet-stream',
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.get(url, headers=headers, stream=True, verify=False)
        response.raise_for_status()

        content_disposition = response.headers.get('content-disposition')
        if content_disposition:
            filename_part = content_disposition.split('filename=')[-1]
            filename = filename_part.strip('"')
        else:
            filename = f"workflow_{workflow_id}_{version_id or 'latest'}.yxzp"

        file_path = os.path.join(temp_dir, filename)

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        # print(f"Workflow '{workflow_id}' package downloaded to: {file_path}") # Moved to process_workflows for better flow
        return file_path

    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP Error downloading workflow {workflow_id}: {e.response.status_code}."
        try:
            error_message += f" Response: {e.response.text}"
        except Exception:
            pass
        if e.response.status_code == 401:
            error_message = f"Unauthorized (401): Could not download workflow {workflow_id}. The access token might be invalid or expired."
        elif e.response.status_code == 403:
            error_message = f"Forbidden (403): Insufficient permissions to download workflow {workflow_id}."
        elif e.response.status_code == 404:
            error_message = f"Not Found (404): Workflow {workflow_id} (version: {version_id or 'latest'}) does not exist."
        raise Exception(error_message) from e
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error downloading workflow {workflow_id}: {e}") from e


def process_workflows(
    workflow_ids: List[str],
    current_client_id: str,
    current_client_secret: str,
    api_base_url: str,
    output_dir: str = "workflows_output"
) -> None:
    """
    Download and extract multiple workflows to a single output directory.
    """
    if not workflow_ids:
        print("No workflow IDs provided to process.")
        return

    os.makedirs(output_dir, exist_ok=True)
    temp_dir = os.path.join(output_dir, "_temp_downloads")
    os.makedirs(temp_dir, exist_ok=True)

    access_token: Optional[str] = None
    try:
        print("Attempting to obtain access token...")
        access_token = get_access_token(current_client_id, current_client_secret, api_base_url)
        print("Successfully obtained access token.")
    except Exception as e:
        print(f"CRITICAL: Could not obtain access token. Aborting. Details: {e}")
        return

    total_workflows = len(workflow_ids)
    print(f"\nStarting processing of {total_workflows} workflows...")
    for index, workflow_id in enumerate(workflow_ids):
        try:
            print(f"\n[{index + 1}/{total_workflows}] Processing workflow ID: {workflow_id}")

            zip_path = download_workflow_package(
                workflow_id,
                access_token,
                api_base_url,
                temp_dir=temp_dir
            )
            print(f"  Downloaded '{workflow_id}' package to: {zip_path}")

            extract_zip(zip_path, output_dir)
            print(f"  Successfully processed and extracted workflow: {workflow_id}")

        except Exception as e:
            print(f"  ERROR processing workflow {workflow_id}: {e}")
            print(f"  Skipping workflow {workflow_id} due to error.")
            continue
    print(f"\nFinished processing all {total_workflows} targeted workflows.")

    try:
        if os.path.exists(temp_dir):
            if not os.listdir(temp_dir):
                os.rmdir(temp_dir)
                print(f"Successfully removed empty temporary directory: {temp_dir}")
            else:
                print(f"Warning: Temporary directory '{temp_dir}' is not empty. Manual cleanup might be needed.")
                # For forceful removal:
                # import shutil
                # shutil.rmtree(temp_dir)
                # print(f"Successfully removed temporary directory and its contents: {temp_dir}")
    except OSError as e:
        print(f"Error removing temporary directory '{temp_dir}': {e}")

# --- Main Execution ---
if __name__ == "__main__":
    if CLIENT_ID == "YOUR_CLIENT_ID_HERE" or CLIENT_SECRET == "YOUR_CLIENT_SECRET_HERE":
        print("ERROR: Please set ALTERYX_CLIENT_ID and ALTERYX_CLIENT_SECRET environment variables,")
        print("or update CLIENT_ID and CLIENT_SECRET constants at the top of the script.")
        sys.exit(1)

    main_output_directory = "downloaded_alteryx_workflows"
    print(f"Starting Alteryx workflow backup utility.")
    print(f"Output directory: '{main_output_directory}'")

    workflow_ids_to_process: List[str] = []
    try:
        print("Fetching all workflow IDs from the server...")
        temp_access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, BASE_URL)
        workflow_ids_to_process = get_all_workflow_ids(temp_access_token, BASE_URL)
    except Exception as e:
        print(f"Could not fetch workflow IDs from server: {e}")
        print("Proceeding without fetching all IDs. You may define workflow_ids_to_process manually if needed.")
        # Example: workflow_ids_to_process = ["your_specific_id1", "your_specific_id2"]

    if not workflow_ids_to_process:
        print("No workflow IDs were fetched or defined. Exiting.")
        sys.exit(0)

    process_workflows(
        workflow_ids_to_process,
        CLIENT_ID,
        CLIENT_SECRET,
        BASE_URL,
        output_dir=main_output_directory
    )
    print("\nScript execution finished.")















######################################################################################################################################################################################
######################################################################################################################################################################################
######################################################################################################################################################################################





#below lets you specify somewhere that your files are currently (can be nested) and will copy just the alteryx workflows into a single directory with no nests
import os
import shutil
def copy_yxmd_files(source_directory, destination_directory, extension=".yxmd"):
    """
    Copies files with a specific extension from a source directory (and its subdirectories)
    to a destination directory without creating subdirectories in the destination.

    Args:
        source_directory (str): The path to the source directory.
        destination_directory (str): The path to the destination directory.
        extension (str): The file extension to look for (e.g., ".yxmd").
    """

    copied_files_count = 0
    found_files_count = 0
    errors_count = 0
    files_in_error = []

    # 1. Validate source directory
    if not os.path.exists(source_directory):
        print(f"Error: Source directory '{source_directory}' does not exist or is not accessible.")
        return
    if not os.path.isdir(source_directory):
        print(f"Error: Source path '{source_directory}' is not a directory.")
        return

    # 2. Ensure destination directory exists, create if not.
    #    Also validate if the destination_directory path given is indeed a directory if it exists.
    try:
        if os.path.exists(destination_directory):
            if not os.path.isdir(destination_directory):
                print(f"Error: Destination path '{destination_directory}' exists but is not a directory.")
                return
        else:
            os.makedirs(destination_directory)
            print(f"Created destination directory: '{destination_directory}'")
    except Exception as e:
        print(f"Error creating or accessing destination directory '{destination_directory}': {e}")
        return

    print(f"\nStarting file copy process...")
    print(f"Source: '{source_directory}'")
    print(f"Destination: '{destination_directory}'")
    print(f"Looking for files with extension: '{extension}'\n")

    # 3. Walk through the source directory
    for root, dirs, files in os.walk(source_directory):
        for filename in files:
            # 4. Check for the specified extension (case-insensitive)
            if filename.lower().endswith(extension.lower()):
                found_files_count += 1
                source_file_path = os.path.join(root, filename)
                
                # The destination for shutil.copy2 will be the destination directory.
                # The file will be copied into this directory with its original name.
                # If a file with the same name already exists in the destination,
                # shutil.copy2 will overwrite it.
                destination_file_path = os.path.join(destination_directory, filename) 

                try:
                    print(f"Copying: '{source_file_path}' \n\t to '{destination_file_path}'...")
                    # 5. Copy the file (absolute guarantee not to touch the source)
                    shutil.copy2(source_file_path, destination_directory) # Copies to the directory
                    copied_files_count += 1
                    print(f"Successfully copied: '{filename}'")
                except shutil.SameFileError:
                    errors_count += 1
                    files_in_error.append(filename + " (SameFileError - source and destination are the same file)")
                    print(f"Error: Source and destination are the same file for '{filename}'. Skipped.")
                except PermissionError:
                    errors_count += 1
                    files_in_error.append(filename + " (PermissionError)")
                    print(f"Error: Permission denied when trying to copy '{filename}'. Skipped.")
                except Exception as e:
                    errors_count += 1
                    files_in_error.append(f"{filename} (Error: {e})")
                    print(f"An unexpected error occurred while copying '{filename}': {e}. Skipped.")
                print("-" * 30)

    # Summary
    print(f"\n--- File Copy Summary ---")
    print(f"Total files found with '{extension}' extension: {found_files_count}")
    print(f"Files successfully copied: {copied_files_count}")
    print(f"Errors encountered during copying: {errors_count}")
    if errors_count > 0:
        print("\nFiles that could not be copied or resulted in an error:")
        for f_error in files_in_error:
            print(f"  - {f_error}")
    print("Process complete.")

# --- Configuration --- 
SOURCE_PROCESS_DIR = None
DESTINATION_HOLDING_DIR = 

FILE_EXTENSION_TO_COPY = ".yxmd"

# --- Run the function ---
#if __name__ == "__main__":
#    copy_yxmd_files(SOURCE_PROCESS_DIR, DESTINATION_HOLDING_DIR, FILE_EXTENSION_TO_COPY)



