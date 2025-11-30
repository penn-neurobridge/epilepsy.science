#%%
import subprocess
import logging
import re
import typer
import pandas as pd
import get_pennseive_datasets as pennseive
import diff_pennseive_datasets as diff_pennseive

from pathlib import Path


# Configure logging to show info messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%

def get_pennseive_dataset_id(dataset_name):
    """
    Get the Pennsieve dataset ID for a given dataset name.
    """
    pennepi_collection = pennseive.main()
    for dataset in pennepi_collection:
        if dataset['name'] == dataset_name:
            dataset_id = dataset['id']
            log.info(f"Found dataset: {dataset_name} with ID: {dataset_id}")
            return dataset_id
    log.error(f"Dataset not found: {dataset_name}")
    return None

def set_pennseive_dataset_active(dataset_id):
    """
    Set the Pennsieve dataset active.
    """
    try:
        result = subprocess.run(['pennsieve', 'dataset', 'use', dataset_id], check=True)
        log.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to set dataset active: {e.stderr}")
        return False

def diff_pennseive_dataset(dataset_name, base_data_dir):
    """
    Diff the Pennsieve dataset.
    """
    
    diff_df = diff_pennseive.diff_dataset(
        dataset_name=dataset_name,
        base_data_dir=base_data_dir)

    # Filter diff_df for ADDED files only
    diff_df = diff_df[diff_df['UPDATE'] == 'ADDED']
    
    return diff_df

def make_manifest(added_files):
    """
    Make a manifest for the Pennsieve dataset.
    """

    first_file = added_files.iloc[0]

    cmd = ['pennsieve', 'manifest', 'create', str(first_file['FULL_PATH'])]
    target_path = str(first_file['PATH'])
    cmd.extend(['-t', target_path])
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    log.info(result.stdout)
    match = re.search(r'Manifest ID:\s*(\d+)', result.stdout)
    manifest_id = match.group(1)

    log.info(f"Manifest ID: {manifest_id} with file {first_file['FILE NAME']} -> {target_path}")

    # grab the manifest id and add the remaining files to the manifest
    for index, file in added_files.iloc[1:].iterrows():
        cmd = ['pennsieve', 'manifest', 'add', manifest_id, str(file['FULL_PATH'])]
        target_path = str(file['PATH'])
        cmd.extend(['-t', target_path])
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        log.info(f"Added {file['FILE NAME']} -> {target_path} to manifest {manifest_id}")

    return manifest_id

def upload_manifest(manifest_id):
    """
    Upload a manifest to the Pennsieve dataset.
    """
    cmd = ['pennsieve', 'upload', 'manifest', manifest_id]
    subprocess.run(cmd, check=True)
    log.info(f"Uploaded manifest {manifest_id}")

# %%
def main(dataset_name: str = typer.Option(..., help="The name of the dataset to push"),
         base_data_dir: str = typer.Option(..., help="The directory where the datasets are mapped")):

    dataset_id = get_pennseive_dataset_id(dataset_name)
    added_files = diff_pennseive_dataset(dataset_name, base_data_dir)

    if len(added_files) == 0:
        log.info("No ADDED files found. Nothing to upload.")
        return
    else:
        set_pennseive_dataset_active(dataset_id)
        manifest_id = make_manifest(added_files)
        upload_manifest(manifest_id)


# %%
if __name__ == "__main__":
    # current_dir = Path(__file__).parent
    # data_dir = current_dir / "data"
    # main(dataset_name="PennEPI00159", base_data_dir=str(data_dir))
    typer.run(main)