"""
Dataset operations for epilepsy science datasets.

This module contains functions for fetching and filtering EPS datasets.
"""
# %%
import logging

from config import Config
from clients import AuthenticationClient, SessionManager
from clients import DatasetsClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# %%
def setup_pennsieve_clients():
    """
    Set up Pennsieve API clients for data access.
    
    Returns:
        DatasetsClient: Client for accessing Pennsieve datasets
    """
    log.info("Setting up Pennsieve clients...")
    config = Config()
    authorization_client = AuthenticationClient(api_host=config.API_HOST)
    session_manager = SessionManager(authorization_client, api_key=config.API_KEY, api_secret=config.API_SECRET)
    datasets_client = DatasetsClient(api_host=config.API_HOST, session_manager=session_manager)
    return datasets_client


def get_PennEPI_datasets(datasets_client):
    """
    Fetch all datasets and filter for PennEPI datasets.
    
    Args:
        datasets_client: Client for accessing Pennsieve datasets
        
    Returns:
        list: List of PennEPI datasets with 'name' and 'id' keys, sorted by name
    """
    log.info("Fetching all datasets from Pennsieve...")
    
    # Get all datasets from the API
    datasets = datasets_client.get_all_datasets()
    
    # Filter for EPS datasets
    pennepi_datasets = []
    for dataset in datasets:
        name = dataset['content']['name']
        id = dataset['content']['id']
        if 'pennepi' in name.lower():
            pennepi_datasets.append({'name': name, 'id': id})
    
    # Sort alphabetically by name
    pennepi_datasets.sort(key=lambda x: x['name'])
    
    log.info(f"Found {len(pennepi_datasets)} PennEPI datasets")
    return pennepi_datasets

def main():
    """Main function when script is run directly."""
    datasets_client = setup_pennsieve_clients()
    pennepi_datasets = get_PennEPI_datasets(datasets_client)
    return pennepi_datasets


# %%
if __name__ == "__main__":
    pennepi_collection = main()
    log.info(f"PennEPI collection: {pennepi_collection}")
