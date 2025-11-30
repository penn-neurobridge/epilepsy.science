#%%
import subprocess
import logging
import typer
import shutil
import get_pennseive_datasets as pennseive

from pathlib import Path


# Configure logging to show info messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%
def map_dataset(dataset_id, dataset_name, base_data_dir="data", remove_existing=False):
    """
    Map a Pennsieve dataset to a local directory.
    
    Args:
        dataset_id (str): The Pennsieve dataset ID
        dataset_name (str): The name of the dataset (used for directory name)
        base_data_dir (str): Base directory where datasets will be stored (default: "data")
    
    Returns:
        bool: True if mapping was successful, False otherwise
    """
    # Create the full path for the dataset directory
    dataset_path = Path(base_data_dir) / "output" / dataset_name
    
    try:
        # Check if dataset directory already then delete it and remap it
        if dataset_path.exists() and remove_existing:
            log.info(f"Dataset '{dataset_name}' is already mapped at {dataset_path}, deleting and remapping...")
            shutil.rmtree(dataset_path)
        elif dataset_path.exists() and not remove_existing:
            log.info(f"Dataset '{dataset_name}' is already mapped at {dataset_path}, skipping...")
            return True
        else:
            log.info(f"Dataset '{dataset_name}' is not mapped, mapping at {dataset_path}")
        
        # Ensure the base data directory exists
        data_dir = Path(base_data_dir)
        data_dir.mkdir(exist_ok=True)
        
        log.info(f"Mapping '{dataset_name}' to {dataset_path}")
        # Run the pennsieve map command
        result = subprocess.run([
            'pennsieve', 
            'map', 
            dataset_id, 
            str(dataset_path)
        ], capture_output=True, text=True, check=True)
        log.info(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to map '{dataset_name}': {e.stderr}")
        return False
        
    except FileNotFoundError:
        log.error("The 'pennsieve' command was not found. Make sure Pennsieve agent is running.")
        return False
        
    except Exception as e:
        log.error(f"Error mapping '{dataset_name}': {e}")
        return False

# %%
def main(
    base_data_dir: str = typer.Option("data", help="The directory where the datasets will be mapped"),
    dataset_name: str = typer.Option("", "--dataset-name", "-n", help="The name(s) of the dataset(s) to map. Can be a single string or a comma-separated list."),
    remove_existing: bool = typer.Option(False, "--remove-existing", "-R", help="Remove existing dataset directory before mapping.")
    ):
    """
    Map Pennsieve datasets to local directories.
    
    Args:
        base_data_dir: The directory where the datasets will be mapped
        dataset_name: The name(s) of the dataset(s) to map. Can be a single string or a comma-separated list.
        remove_existing: Remove existing dataset directory before mapping.
    """
    # Get all PennEPI datasets
    pennepi_collection = pennseive.main()

    # Filter datasets if dataset_name is provided
    if dataset_name:
        # Handle comma-separated string by splitting it
        if "," in dataset_name:
            # Split by comma and strip whitespace from each name
            filter_names = [name.strip() for name in dataset_name.split(",")]
        else:
            # Single dataset name
            filter_names = [dataset_name]
        
        # Filter the collection to only include matching datasets
        pennepi_collection = [
            dataset for dataset in pennepi_collection 
            if dataset['name'] in filter_names
        ]
        
        # Log if no datasets matched
        if not pennepi_collection:
            log.warning(f"No datasets found matching: {filter_names}")
            return
    
    # Map each dataset in the collection
    for dataset in pennepi_collection:
        map_dataset(dataset_id=dataset['id'], dataset_name=dataset['name'], base_data_dir=base_data_dir, remove_existing=remove_existing)
# %%
if __name__ == "__main__":
    typer.run(main)

# %%
