# %% 
import logging

from config import Config
from clients import AuthenticationClient, SessionManager
from clients import DatasetsClient
from utils import GoogleSheetsClient, UtilsConfig

from dataset_operations import get_eps_datasets
from data_processing import (
    get_migration_tracker, 
    merge_hupid_to_eps_datasets, 
    merge_rid_to_eps_datasets,
    prepare_rid_hupid_data
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


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


def setup_google_sheets_client():
    """
    Set up Google Sheets client for accessing external data.
    
    Returns:
        GoogleSheetsClient: Client for accessing Google Sheets data
    """
    log.info("Setting up Google Sheets client...")
    utils_config = UtilsConfig()
    sheets_client = GoogleSheetsClient(utils_config)
    return sheets_client


def enrich_with_hup_ids(eps_datasets, sheets_client):
    """
    Enrich EPS datasets with HUP IDs from migration tracker.
    
    Args:
        eps_datasets: List of EPS dataset dictionaries
        sheets_client: Google Sheets client
        
    Returns:
        list: EPS datasets enriched with HUP IDs
    """
    log.info("Enriching datasets with HUP IDs...")
    migration_tracker = get_migration_tracker(sheets_client)
    return merge_hupid_to_eps_datasets(eps_datasets, migration_tracker)


def enrich_with_record_ids(eps_datasets, sheets_client):
    """
    Enrich EPS datasets with record IDs.
    
    Args:
        eps_datasets: List of EPS dataset dictionaries
        sheets_client: Google Sheets client
        
    Returns:
        list: EPS datasets enriched with record IDs
    """
    log.info("Enriching datasets with record IDs...")
    rid_hupid = prepare_rid_hupid_data(sheets_client)
    return merge_rid_to_eps_datasets(eps_datasets, rid_hupid)


def get_enriched_eps_datasets():
    """
    Main function to get and enrich EPS datasets with all available IDs.
    
    Returns:
        list: Fully enriched EPS datasets with HUP IDs and record IDs
    """
    # Set up clients
    datasets_client = setup_pennsieve_clients()
    sheets_client = setup_google_sheets_client()
    
    # Get base EPS datasets
    log.info("Fetching EPS datasets...")
    eps_datasets = get_eps_datasets(datasets_client)
    
    # Enrich with additional IDs
    eps_datasets = enrich_with_hup_ids(eps_datasets, sheets_client)
    eps_datasets = enrich_with_record_ids(eps_datasets, sheets_client)
    
    # Log summary
    log.info(f"EPS datasets enriched with hup_id and record_id")
    log.info(f"Total EPS datasets: {len(eps_datasets)}")
    log.info(f"Datasets with HUP ID: {sum(1 for d in eps_datasets if 'hup_id' in d)}")
    log.info(f"Datasets with record ID: {sum(1 for d in eps_datasets if 'record_id' in d)}")
    
    return eps_datasets


# Create a module-level variable for easy import
eps_datasets = None


def main():
    """Main function when script is run directly."""
    global eps_datasets
    eps_datasets = get_enriched_eps_datasets()
    return eps_datasets


# %%
if __name__ == "__main__":
    main()