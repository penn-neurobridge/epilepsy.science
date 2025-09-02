#%%
import logging
import backoff
import subprocess
import json

from pathlib import Path
from config import Config
from clients import AuthenticationClient, SessionManager
from clients import PackageClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%
def setup_pennsieve_clients():
    """
    Set up Pennsieve API clients for data access.
    
    Returns:
        PackageClient: Client for downloading Pennsieve packages
    """
    log.info("Setting up Pennsieve Package Download Client...")
    config = Config()
    authorization_client = AuthenticationClient(api_host=config.API_HOST)
    session_manager = SessionManager(authorization_client, api_key=config.API_KEY, api_secret=config.API_SECRET)
    package_client = PackageClient(api_host=config.API_HOST, session_manager=session_manager)
    return package_client

@backoff.on_exception(
    backoff.expo,  # Exponential backoff
    (subprocess.CalledProcessError, OSError),  # Handle subprocess and OS errors
    max_tries=4,  # Try up to 4 times total
    max_time=300,  # Don't retry for more than 5 minutes
    on_backoff=lambda details: log.warning(f"Download failed, retrying in {details['wait']:.1f}s (attempt {details['tries']}/{details['max_tries']})")
)
def download_file_with_curl_backoff(url, output_path):
    """
    Download a file using curl with automatic retry using exponential backoff.
    
    Args:
        url: The URL to download from
        output_path: Local path where the file should be saved
    
    Returns:
        subprocess.CompletedProcess: The result of the curl command
    """
    log.debug(f"Attempting download from {url[:100]}...")  # Log first 100 chars of URL
    
    # Use curl with options optimized for large file downloads
    result = subprocess.run([
        'curl', 
        '-L',  # Follow redirects
        '--progress-meter',  # Show progress
        '--fail',  # Fail on HTTP errors (4xx, 5xx)
        '--connect-timeout', '30',  # Connection timeout
        '--retry', '0',  # Don't let curl do its own retries (we handle this)
        '-o', str(output_path),  # Output file
        url
    ], text=True, check=True)
    
    log.info(f"Successfully downloaded file to {output_path}")
    return result

def process_files_and_download(file_path, package_client):
    """
    Process either a single file or all files in a directory and download them.
    
    Args:
        file_path (str or Path): Path to a file or directory containing package IDs
        package_client: The Pennsieve package client for getting download URLs
    """
    path = Path(file_path)
    
    # Check if path exists
    if not path.exists():
        log.error(f"Path does not exist: {path}")
        return
    
    # Find the manifest.json file to validate package IDs
    manifest_path = find_manifest_file(path)
    if not manifest_path:
        log.error("Could not find manifest.json file. Cannot validate package IDs.")
        return
    
    # Load valid package IDs from manifest
    valid_package_ids = load_valid_package_ids(manifest_path)
    log.info(f"Loaded {len(valid_package_ids)} valid package IDs from manifest")
    
    # Determine if it's a file or directory
    if path.is_file():
        log.info(f"Processing single file: {path}")
        files_to_process = [path]
    elif path.is_dir():
        log.info(f"Processing directory: {path}")
        # Get all files in the directory and subdirectories recursively
        files_to_process = [f for f in path.rglob('*') if f.is_file()]
        log.info(f"Found {len(files_to_process)} files to process")
    else:
        log.error(f"Path is neither a file nor a directory: {path}")
        return
    
    # Process each file
    for file_path in files_to_process:
        try:
            log.info(f"Processing file: {file_path}")
            
            # Read the content from the file
            with open(file_path, 'r') as f:
                content = f.read().strip()  # strip() removes any whitespace
            
            # Validate if this is a valid package ID by checking against manifest
            if content not in valid_package_ids:
                log.info(f"Skipping {file_path.name} - already downloaded (package ID not found in manifest or invalid)")
                continue
            
            package_id = content
            log.info(f"Valid package ID found: {package_id}")
            
            # Get the download manifest from Pennsieve
            response = package_client.get_download_manifest(package_id)
            presigned_url = response['data'][0]['url']
            
            # Download the file using curl with backoff
            log.info(f"Downloading to {file_path}")
            download_file_with_curl_backoff(presigned_url, file_path)
            
        except Exception as e:
            log.error(f"Error processing file {file_path}: {str(e)}")
            continue  # Continue with the next file even if one fails

def find_manifest_file(start_path):
    """
    Find the manifest.json file by searching up the directory tree.
    
    Args:
        start_path (Path): The starting path to search from
    
    Returns:
        Path or None: Path to manifest.json if found, None otherwise
    """
    current_path = start_path if start_path.is_dir() else start_path.parent
    
    # Search up the directory tree for .pennsieve/manifest.json
    while current_path != current_path.parent:  # Stop at root directory
        manifest_path = current_path / '.pennsieve' / 'manifest.json'
        if manifest_path.exists():
            log.info(f"Found manifest file: {manifest_path}")
            return manifest_path
        current_path = current_path.parent
    
    return None

def load_valid_package_ids(manifest_path):
    """
    Load all valid package IDs from the manifest.json file.
    
    Args:
        manifest_path (Path): Path to the manifest.json file
    
    Returns:
        set: Set of valid package IDs
    """
    try:
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
        
        # Extract package IDs from the manifest
        valid_ids = set()
        for file_info in manifest_data.get('files', []):
            package_id = file_info.get('packageId', '')
            # Remove the "N:package:" prefix if present
            if package_id.startswith('N:package:'):
                package_id = package_id[10:]  # Remove "N:package:" prefix
            valid_ids.add(package_id)
        
        return valid_ids
        
    except Exception as e:
        log.error(f"Error reading manifest file {manifest_path}: {str(e)}")
        return set()

#%%
if __name__ == "__main__":
    # Set up the Pennsieve client
    package_client = setup_pennsieve_clients()
    
    # Define the path to process (can be a file or directory)
    input_path = '/Users/nishant/Dropbox/Sinha/Lab/Research/epilepsy_science_curate/data/pennsieve/EPS0000128'
    
    # Process the files and download them
    process_files_and_download(input_path, package_client)

#%%