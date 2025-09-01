#%%
import logging
import requests  # Added missing import
import backoff  # Add backoff for retry logic
import subprocess

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
        '--progress-bar',  # Show progress bar
        '--fail',  # Fail on HTTP errors (4xx, 5xx)
        '--connect-timeout', '30',  # Connection timeout
        '--max-time', '600',  # Max time for entire operation (10 minutes)
        '--retry', '0',  # Don't let curl do its own retries (we handle this)
        '-o', str(output_path),  # Output file
        url
    ], text=True, check=True)
    
    log.info(f"Successfully downloaded file to {output_path}")
    return result

#%%
if __name__ == "__main__":
    package_client = setup_pennsieve_clients()

    file_path = Path('/Users/nishant/Dropbox/Sinha/Lab/Research/epilepsy_science_curate/data/pennsieve/EPS0000001/primary/sub-EPS0000001/ses-postimplant/ieeg/F8.mef')

    with open(file_path, 'r') as f:
        package_id = f.read()

    response = package_client.get_download_manifest(package_id)
    presigned_url = response['data'][0]['url']
    
    # Use curl with backoff for reliable binary file downloads
    log.info(f"Downloading {file_path}")
    download_file_with_curl_backoff(presigned_url, file_path)


#%%