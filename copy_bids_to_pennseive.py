import shutil
from pathlib import Path
import logging
import typer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%

def copy_ieeg_recon(source_dir: Path, target_dir: Path, name_derivative: str):
    """
    Copy ieeg_recon derivative to Pennsieve.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
        name_derivative (str): The name of the derivative to copy
    """

    ieeg_recon_dir = source_dir.rglob(f'{name_derivative}').__next__()
    target_ieeg_recon_dir = target_dir / 'derivatives' / 'ieeg_recon'
    shutil.copytree(ieeg_recon_dir, target_ieeg_recon_dir, dirs_exist_ok=True)



#%%

def main(source_dir: Path, target_dir: Path, name_derivative: str):
    """
    Copy BIDS data to Pennsieve.
    
    Args:
        source_dir (str): The source directory where the data is stored
        target_dir (str): The target directory where the data will be copied to
        name_derivative (str): The name of the derivative to copy
    """

    log.info(f"Copying data from {source_dir} to {target_dir}")

    if name_derivative == "ieeg_recon":
        copy_ieeg_recon(source_dir, target_dir, name_derivative)

#%%

if __name__ == "__main__":

    # Set this to True for testing with hard-coded values
    # Set to False to use command-line arguments
    USE_HARDCODED_VALUES = True

    if USE_HARDCODED_VALUES:
        source_dir = Path('/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS/sub-RID0054')
        target_dir = Path('/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data/output/PennEPI00143')
        name_derivative = "ieeg_recon"
        main(source_dir=source_dir, target_dir=target_dir, name_derivative=name_derivative)
        log.info(f"Done copying {name_derivative} from {source_dir} to {target_dir}")
    else:
        typer.run(main)
