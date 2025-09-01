"""
Data processing functions for EPS datasets.

This module contains functions for loading data from Google Sheets
and merging different types of IDs into the EPS datasets.
"""

import logging

log = logging.getLogger(__name__)


def get_migration_tracker(sheets_client):
    """
    Load migration tracker data from Google Sheets.
    
    Args:
        sheets_client: Client for accessing Google Sheets
        
    Returns:
        DataFrame: Migration tracker with 'label' and 'EPS Number' columns
    """
    log.info("Loading migration tracker from Google Sheets...")
    
    migration_tracker = sheets_client.read_migration_tracker()
    # Keep only the columns we need
    migration_tracker = migration_tracker[['label', 'EPS Number']]
    
    log.info(f"Loaded migration tracker with {len(migration_tracker)} rows")
    return migration_tracker


def merge_hupid_to_eps_datasets(eps_datasets, migration_tracker):
    """
    Add HUP IDs to EPS datasets using migration tracker data.
    
    This function:
    1. Looks up each EPS dataset name in the migration tracker
    2. Extracts the HUP ID number from the corresponding label
    3. Adds the HUP ID to the dataset dictionary
    
    Args:
        eps_datasets: List of EPS dataset dictionaries
        migration_tracker: DataFrame with migration tracking data
        
    Returns:
        list: The same dataset list with HUP IDs added where found
    """
    log.info("Merging HUP IDs to EPS datasets...")
    
    for dataset in eps_datasets:
        if dataset['name'] in migration_tracker['EPS Number'].values:
            # Get the HUP label for this EPS dataset
            hup_label = migration_tracker[migration_tracker['EPS Number'] == dataset['name']]['label'].values[0]
            
            # Extract HUP ID from the label
            # Example: "HUP123_session1" -> get the number 123
            hup_id = hup_label.split('_')[0]  # "HUP123"
            hup_id = ''.join(filter(str.isdigit, hup_id))  # "123"
            hup_id = int(hup_id)  # 123
            
            # Add HUP ID to the dataset
            dataset['hup_id'] = hup_id
            
            # Update the dataset in the list (this line was in original but not needed)
            eps_datasets[eps_datasets.index(dataset)]['hup_id'] = hup_id
    
    hup_count = sum(1 for d in eps_datasets if 'hup_id' in d)
    log.info(f"Merged HUP IDs for {hup_count} out of {len(eps_datasets)} datasets")
    return eps_datasets


def merge_rid_to_eps_datasets(eps_datasets, rid_hupid):
    """
    Add RID record IDs to EPS datasets using RID-HupID mapping.
    
    This function:
    1. For datasets that have HUP IDs, looks them up in the RID-HupID mapping
    2. Finds the corresponding RID record ID
    3. Adds the record ID to the dataset dictionary
    
    Args:
        eps_datasets: List of EPS dataset dictionaries (should have HUP IDs)
        rid_hupid: DataFrame with RID-HupID mapping data
        
    Returns:
        list: The same dataset list with record IDs added where found
    """
    log.info("Merging RID record IDs to EPS datasets...")
    
    success_count = 0
    error_count = 0
    
    for dataset in eps_datasets:
        try:
            # Only process datasets that have a HUP ID
            if 'hup_id' in dataset and dataset['hup_id'] in rid_hupid['hupsubjno'].values:
                # Find the record ID for this HUP ID
                record_id = rid_hupid[rid_hupid['hupsubjno'] == dataset['hup_id']]['record_id'].values[0]
                dataset['record_id'] = record_id
                
                # Update dataset in the list (this line was in original but not needed)
                eps_datasets[eps_datasets.index(dataset)]['record_id'] = record_id
                success_count += 1
                
        except Exception as e:
            log.error(f"Error merging RID for dataset {dataset['name']}: {e}")
            error_count += 1
            continue
    
    log.info(f"Merged record IDs for {success_count} datasets")
    if error_count > 0:
        log.warning(f"Encountered errors for {error_count} datasets")
        
    return eps_datasets


def prepare_rid_hupid_data(sheets_client):
    """
    Load and clean RID-HupID mapping data from Google Sheets.
    
    This function handles the data cleaning steps:
    1. Remove rows where hupsubjno is missing
    2. Convert hupsubjno to integer
    3. Format record_id with proper prefix and padding
    
    Args:
        sheets_client: Client for accessing Google Sheets
        
    Returns:
        DataFrame: Cleaned RID-HupID mapping data
    """
    log.info("Loading RID-HupID mapping from Google Sheets...")
    
    # Load the raw data
    rid_hupid = sheets_client.read_rid_hupid_sheet()
    
    # Clean the data
    rid_hupid = rid_hupid[rid_hupid['hupsubjno'].notna()]  # Remove missing values
    rid_hupid['hupsubjno'] = rid_hupid['hupsubjno'].astype(int)  # Convert to integer
    
    # Format record_id: add 'sub-RID' prefix and zero-pad to 4 digits
    # Example: 123 becomes 'sub-RID0123'
    rid_hupid['record_id'] = 'sub-RID' + rid_hupid['record_id'].astype(str).str.zfill(4)
    
    log.info(f"Loaded and cleaned RID-HupID mapping with {len(rid_hupid)} rows")
    return rid_hupid 