# %% 
import os
import logging
import pandas as pd

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

log = logging.getLogger()

# %%
if __name__ == "__main__":
    # Load Pennsieve configuration
    config = Config()
    authorization_client = AuthenticationClient(api_host=config.API_HOST)
    session_manager = SessionManager(authorization_client, api_key=config.API_KEY, api_secret=config.API_SECRET)
    datasets_client = DatasetsClient(api_host=config.API_HOST, session_manager=session_manager)
    
    # Get EPS datasets
    eps_datasets = get_eps_datasets(datasets_client)

    # Load utilities configuration and use Google Sheets client
    utils_config = UtilsConfig()
    sheets_client = GoogleSheetsClient(utils_config)
    
    # Get migration tracker and merge HUP IDs
    migration_tracker = get_migration_tracker(sheets_client)
    eps_datasets = merge_hupid_to_eps_datasets(eps_datasets, migration_tracker)

    # Get RID-HupID mapping and merge record IDs
    rid_hupid = prepare_rid_hupid_data(sheets_client)
    eps_datasets = merge_rid_to_eps_datasets(eps_datasets, rid_hupid)

# %%