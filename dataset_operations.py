"""
Dataset operations for epilepsy science datasets.

This module contains functions for fetching and filtering EPS datasets.
"""

import logging

log = logging.getLogger(__name__)


def get_eps_datasets(datasets_client):
    """
    Fetch all datasets and filter for epilepsy science (EPS) datasets.
    
    Args:
        datasets_client: Client for accessing Pennsieve datasets
        
    Returns:
        list: List of EPS datasets with 'name' and 'id' keys, sorted by name
    """
    log.info("Fetching all datasets from Pennsieve...")
    
    # Get all datasets from the API
    datasets = datasets_client.get_all_datasets()
    
    # Filter for EPS datasets
    eps_datasets = []
    for dataset in datasets:
        name = dataset['content']['name']
        id = dataset['content']['id']
        if 'EPS' in name:
            eps_datasets.append({'name': name, 'id': id})
    
    # Sort alphabetically by name
    eps_datasets.sort(key=lambda x: x['name'])
    
    log.info(f"Found {len(eps_datasets)} epilepsy.science datasets")
    return eps_datasets 