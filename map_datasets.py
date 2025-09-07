#%%
import subprocess
import logging
import shutil
from pathlib import Path

from enrich_eps_datasets import get_enriched_eps_datasets

# Configure logging to show info messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#%%
def map_dataset(dataset_id, dataset_name, base_data_dir="data"):
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
    dataset_path = Path.cwd() / base_data_dir / "pennsieve" / dataset_name
    
    try:
        # Check if dataset directory already exists and remove it
        if dataset_path.exists():
            shutil.rmtree(dataset_path)
        
        # Ensure the base data directory exists
        data_dir = Path.cwd() / base_data_dir
        data_dir.mkdir(exist_ok=True)
        
        logger.info(f"Mapping '{dataset_name}' to {dataset_path}")
        # Run the pennsieve map command
        result = subprocess.run([
            'pennsieve', 
            'map', 
            dataset_id, 
            str(dataset_path)
        ], capture_output=True, text=True, check=True)
        logger.info(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to map '{dataset_name}': {e.stderr}")
        return False
        
    except FileNotFoundError:
        logger.error("The 'pennsieve' command was not found. Make sure Pennsieve agent is running.")
        return False
        
    except Exception as e:
        logger.error(f"Error mapping '{dataset_name}': {e}")
        return False

# %%
if __name__ == "__main__":
    eps_datasets = get_enriched_eps_datasets()
    
    for dataset in eps_datasets:
        try:
            if dataset['record_id'] in 'sub-RID0031':
                map_dataset(dataset['id'], dataset['name'])
        except:
            pass
# %%
