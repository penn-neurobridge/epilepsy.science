#%%
import subprocess
import logging
import typer
import pandas as pd
from io import StringIO

from pathlib import Path


# Configure logging to show info messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%
def push_dataset(dataset_name, base_data_dir="data"):
    """
    Push local changes in Pennsieve dataset to remote.
    
    Args:
        dataset_name (str): The name of the dataset (used for directory name)
        base_data_dir (str): Base directory where datasets will be stored (default: "data")
    
    Returns:
        pd.DataFrame or None: DataFrame with changed files, or None if error occurred
    """
    # Create the full path for the dataset directory
    dataset_path = Path(base_data_dir) / "output" / dataset_name
    
    try:
        
        log.info(f"Pushing changes in '{dataset_name}' to remote")
        # Run the pennsieve map command
        result = subprocess.run([
            'pennsieve', 
            'map',
            'push', 
            str(dataset_path)
        ], input='y\n', text=True, check=True)
        log.info(result.stdout)

        # to monitor progress and stop the container from exiting
        subprocess.run(['pennsieve', 'agent', 'subscribe'], check=True)
        
        return True
        
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to push changes in '{dataset_name}': {e.stderr}")
        return None
        
    except FileNotFoundError:
        log.error("The 'pennsieve' command was not found. Make sure Pennsieve agent is running.")
        return None
        
    except Exception as e:
        log.error(f"Error pushing changes in '{dataset_name}': {e}")
        import traceback
        log.error(traceback.format_exc())
        return None

# %%
def main(
    base_data_dir: str = typer.Option("data", help="The directory where the datasets are mapped"),
    dataset_name: str = typer.Option(..., help="The name of the dataset to push"),
):
    """
    Push local changes in Pennsieve dataset to remote.
    
    Args:
        base_data_dir: The directory where the datasets are mapped
        dataset_name: The name of the dataset to push
    """
    
    # Push the dataset
    push_dataset(dataset_name, base_data_dir)
    
# %%
if __name__ == "__main__":
    # Use typer for command-line arguments
    typer.run(main)