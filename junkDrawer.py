#####################################################################################
#Pull workflows from Server and unpack#
#####################################################################################

import requests
from requests.auth import HTTPBasicAuth
import warnings
from urllib3.exceptions import InsecureRequestWarning
import zipfile
import os
import sys
from typing import List, Optional, Set # Added Set
import time


# --- Configuration ---
CLIENT_ID: str = "YOUR_CLIENT_ID_HERE"  # Replace with your Alteryx Gallery Client ID
CLIENT_SECRET: str = "YOUR_CLIENT_SECRET_HERE"  # Replace with your Alteryx Gallery Client Secret
BASE_URL: str = "YOUR_BASE_URL_HERE" 



# --- Functions --- (extract_zip, get_access_token remain the same as your last version)

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
    print(f"Requesting access token from: {token_url}")
    try:
        response = requests.post(
            token_url,
            auth=HTTPBasicAuth(client_id, client_secret),
            data={'grant_type': 'client_credentials'},
            verify=False,
            timeout=REQUEST_TIMEOUT
        )
        print(f"Access token response status: {response.status_code}")
        response.raise_for_status()
        token_data = response.json()
        if 'access_token' not in token_data:
            raise KeyError("'access_token' not in response from token endpoint.")
        print("Access token obtained successfully.")
        return token_data['access_token']
    except requests.exceptions.Timeout:
        raise Exception(f"Timeout occurred while trying to get access token from {token_url}")
    except requests.exceptions.HTTPError as e:
        error_message = f"Failed to get access token. Status Code: {e.response.status_code}. Response: {e.response.text}"
        if e.response.status_code == 401:
            error_message = "Failed to get access token: Unauthorized (401). Check Client ID and Secret."
        elif e.response.status_code == 403:
            error_message = "Failed to get access token: Forbidden (403). Check permissions."
        raise Exception(error_message) from e
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to get access token due to a network or request issue: {e}") from e
    except (KeyError, ValueError) as e: # ValueError for JSON decoding issues
        raise Exception(f"Failed to get access token: Error parsing token response or missing key. Details: {e}")


def get_all_workflow_ids(access_token: str, api_base_url: str, view: Optional[str] = "Default") -> List[str]:
    """
    Retrieve all workflow IDs from the Alteryx server, handling pagination robustly.
    """
    all_workflow_ids_set: Set[str] = set() # Use a set to automatically handle duplicates
    workflows_url = f"{api_base_url}/v3/workflows"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    limit = 100
    offset = 0
    page_count = 0
    consecutive_empty_new_ids_batches = 0 # Counter for batches that add no new unique IDs
    max_consecutive_empty_new_ids = 3     # Threshold to break if we keep getting old data

    print(f"Starting to fetch all workflow IDs from: {workflows_url}")

    while True:
        page_count += 1
        params = {
            'limit': limit,
            'offset': offset
        }
        if view:
            params['view'] = view

        print(f"  Page {page_count}: Fetching workflows with offset: {offset}, limit: {limit}...")
        try:
            response = requests.get(
                workflows_url,
                headers=headers,
                params=params,
                verify=False,
                timeout=REQUEST_TIMEOUT
            )
            print(f"    Response Status: {response.status_code}")
            response.raise_for_status() # Check for HTTP errors like 4xx, 5xx
            
            # Check for empty or non-JSON response before attempting .json()
            if not response.content:
                print("    WARNING: Received empty response content. Assuming end of list.")
                break
            try:
                workflows_batch = response.json()
            except requests.exceptions.JSONDecodeError as json_e:
                print(f"    ERROR: Failed to decode JSON response at offset {offset}. Content: {response.text[:500]}...")
                raise Exception(f"JSONDecodeError at offset {offset}: {json_e}") from json_e


            if not isinstance(workflows_batch, list):
                print(f"    WARNING: Unexpected response format at offset {offset}. Expected a list, got {type(workflows_batch)}.")
                print(f"    Response content: {response.text[:500]}...")
                break # Stop if the format is not a list as expected

            if not workflows_batch: # Standard way to indicate end of data
                print("    No more workflows returned in this batch (empty list). End of list.")
                break

            current_batch_ids = [
                wf.get("id") for wf in workflows_batch
                if wf and isinstance(wf, dict) and wf.get("id") and isinstance(wf.get("id"), str)
            ]
            
            if not current_batch_ids and workflows_batch:
                print(f"    WARNING: Batch from offset {offset} was not empty but yielded no valid workflow IDs. Content: {str(workflows_batch)[:200]}...")
                # This could be an error page formatted as a list of non-workflow objects.
                # We'll rely on other checks to break if this persists.

            initial_set_size = len(all_workflow_ids_set)
            for wf_id in current_batch_ids:
                all_workflow_ids_set.add(wf_id)
            newly_added_count = len(all_workflow_ids_set) - initial_set_size

            print(f"    Fetched {len(workflows_batch)} items, extracted {len(current_batch_ids)} potential IDs. Added {newly_added_count} new unique IDs.")
            print(f"    Total unique IDs so far: {len(all_workflow_ids_set)}.")

            # If a full batch was received but no new unique IDs were added,
            # it's a strong sign we are re-fetching old data.
            if newly_added_count == 0 and len(workflows_batch) > 0 : # len(workflows_batch) > 0 ensures it's not an intentionally empty final page
                consecutive_empty_new_ids_batches += 1
                print(f"    WARNING: No new unique IDs added from this batch. Consecutive such batches: {consecutive_empty_new_ids_batches}.")
                if consecutive_empty_new_ids_batches >= max_consecutive_empty_new_ids:
                    print(f"    ERROR: No new unique IDs for {max_consecutive_empty_new_ids} consecutive non-empty batches. Breaking loop to prevent re-fetching.")
                    break
            else:
                consecutive_empty_new_ids_batches = 0 # Reset counter if new IDs were found

            # Standard break condition: if API returns fewer items than requested limit, it's the last page.
            if len(workflows_batch) < limit:
                print(f"    Last page of workflows reached (received {len(workflows_batch)} items, limit was {limit}).")
                break

            offset += limit # Standard pagination: advance offset by the limit for the next page.
                            # If the API re-serves the same last page, the "no new unique IDs" check above should catch it.

        except requests.exceptions.Timeout:
            print(f"    ERROR: Timeout occurred while fetching workflows at offset {offset}.")
            raise Exception(f"Timeout fetching workflows (offset {offset})") from None
        except requests.exceptions.HTTPError as e:
            error_message = f"    HTTP Error fetching workflows (offset {offset}): {e.response.status_code}."
            try: error_message += f" Response: {e.response.text[:500]}..."
            except Exception: pass
            if e.response.status_code == 401:
                error_message = "    Unauthorized (401) fetching workflows. Access token might be invalid/expired."
            print(error_message)
            raise Exception(error_message) from e
        except requests.exceptions.RequestException as e:
            print(f"    Network error fetching workflows (offset {offset}): {e}")
            raise Exception(f"Network error fetching workflows (offset {offset}): {e}") from e
        # JSONDecodeError is handled above after checking response.content

    final_ids_list = list(all_workflow_ids_set)
    print(f"Finished fetching. Total unique workflow IDs found: {len(final_ids_list)}")
    return final_ids_list


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
    print(f"    Downloading package for workflow ID: {workflow_id} from {url}")
    try:
        response = requests.get(
            url,
            headers=headers,
            stream=True,
            verify=False,
            timeout=REQUEST_TIMEOUT * 10 # Allow more time for larger downloads, e.g., 5 minutes
        )
        print(f"      Package download response status: {response.status_code}")
        response.raise_for_status()

        content_disposition = response.headers.get('content-disposition')
        filename = f"workflow_{workflow_id}_{version_id or 'latest'}.yxzp" # Default filename
        if content_disposition:
            # Basic parsing for filename="filename.yxzp"
            parts = content_disposition.split('filename=')
            if len(parts) > 1:
                extracted_filename = parts[1].strip('"')
                if extracted_filename: # Ensure it's not empty
                    filename = extracted_filename
        
        file_path = os.path.join(temp_dir, filename)

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return file_path
    except requests.exceptions.Timeout:
        raise Exception(f"Timeout occurred while downloading package for workflow {workflow_id}")
    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP Error downloading workflow {workflow_id}: {e.response.status_code}."
        try: error_message += f" Response: {e.response.text[:500]}..."
        except Exception: pass
        if e.response.status_code == 401: error_message = f"Unauthorized (401) downloading workflow {workflow_id}. Token invalid/expired?"
        elif e.response.status_code == 403: error_message = f"Forbidden (403) downloading workflow {workflow_id}."
        elif e.response.status_code == 404: error_message = f"Not Found (404) for workflow {workflow_id} (version: {version_id or 'latest'})."
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
        access_token = get_access_token(current_client_id, current_client_secret, api_base_url)
    except Exception as e:
        print(f"CRITICAL: Could not obtain access token for processing. Aborting. Details: {e}")
        return

    total_workflows = len(workflow_ids)
    print(f"\nStarting processing of {total_workflows} workflows...")
    success_count = 0
    failure_count = 0
    for index, workflow_id in enumerate(workflow_ids):
        print(f"\n[{index + 1}/{total_workflows}] Processing workflow ID: {workflow_id}")
        try:
            zip_path = download_workflow_package(
                workflow_id,
                access_token, # type: ignore
                api_base_url,
                temp_dir=temp_dir
            )
            print(f"  Downloaded '{workflow_id}' package to: {zip_path}")
            extract_zip(zip_path, output_dir)
            print(f"  Successfully processed and extracted workflow: {workflow_id}")
            success_count += 1
        except Exception as e:
            print(f"  ERROR processing workflow {workflow_id}: {e}")
            failure_count +=1
            if "401" in str(e) or "Unauthorized" in str(e) or "token might be invalid/expired" in str(e).lower() :
                print("  Access token might have expired. Attempting to refresh token...")
                try:
                    access_token = get_access_token(current_client_id, current_client_secret, api_base_url)
                    print("  Successfully refreshed access token. You may need to re-run for the failed workflow.")
                except Exception as token_e:
                    print(f"  CRITICAL: Failed to refresh access token. Aborting further processing. Error: {token_e}")
                    break
            continue # Continue to next workflow even if one fails (unless token refresh fails critically)

    print(f"\nFinished processing workflows.")
    print(f"Successfully processed: {success_count}")
    print(f"Failed to process: {failure_count}")

    try:
        if os.path.exists(temp_dir):
            if not os.listdir(temp_dir):
                os.rmdir(temp_dir)
                print(f"Successfully removed empty temporary directory: {temp_dir}")
            else:
                print(f"Warning: Temporary directory '{temp_dir}' is not empty. Contents: {os.listdir(temp_dir)}. Manual cleanup might be needed.")
    except OSError as e:
        print(f"Error removing temporary directory '{temp_dir}': {e}")

# --- Main Execution ---
if __name__ == "__main__":
    main_output_directory = "downloaded_alteryx_workflows"
    print(f"Starting Alteryx workflow backup utility.")
    print(f"Client ID: {CLIENT_ID[:4]}...{CLIENT_ID[-4:]}")
    print(f"Base URL: {BASE_URL}")
    print(f"Output directory: '{main_output_directory}'")

    workflow_ids_to_process: List[str] = []
    access_token_for_listing: Optional[str] = None

    try:
        print("\nStep 1: Obtaining access token for listing workflows...")
        access_token_for_listing = get_access_token(CLIENT_ID, CLIENT_SECRET, BASE_URL)
    except Exception as e:
        print(f"CRITICAL: Could not obtain initial access token: {e}")
        sys.exit(1)

    if access_token_for_listing: # Ensure token was obtained
        try:
            print("\nStep 2: Fetching all workflow IDs from the server...")
            workflow_ids_to_process = get_all_workflow_ids(access_token_for_listing, BASE_URL)
        except Exception as e:
            print(f"ERROR: Could not fetch workflow IDs from server: {e}")
            print("Proceeding without a list of workflow IDs. You may need to investigate the API or script.")
            # sys.exit(1) # Or exit if this step is critical
    else: # Should not happen if get_access_token raises exception, but as a safeguard.
        print("CRITICAL: Access token for listing was not obtained. Cannot fetch workflow IDs.")
        sys.exit(1)


    if not workflow_ids_to_process:
        print("\nNo workflow IDs were fetched or found. Exiting.")
        sys.exit(0)

    print(f"\nStep 3: Processing {len(workflow_ids_to_process)} unique workflows...")
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



