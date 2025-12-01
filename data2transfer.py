from pathlib import Path
import logging
import subprocess
import typer
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


#%%

def rsync_data(source_dir: Path, target_dir: Path):
    """
    Copy original BIDS data to BIDS_data directory.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    exclude_list = ['.DS_Store',
                'derivatives/connectivityDWI', 
                'derivatives/connectivityIEEG', 
                'derivatives/preprocessDWI', 
                'derivatives/ieeg-portal-clips',
                'derivatives/derivatives_b300',
                'derivatives/derivatives_b700',
                'derivatives/derivatives_b2000',
                'derivatives/freesurfer/scripts',
                'derivatives/freesurfer/tmp',
                'derivatives/freesurfer/touch',
                'derivatives/freesurfer/trash',
                'derivatives/freesurfer/stats',
                'derivatives/freesurfer/cvs',
                'derivatives/freesurfer/CT',
                'derivatives/freesurfer/logs',
                'derivatives/freesurfer/xhemi',
                'derivatives/freesurfer/noddi',
                'derivatives/ieeg_recon'] # exclude these directories from the copy
                
    # Create a list where each exclude is a separate argument
    exclude_args = [f'--exclude={item}' for item in exclude_list]
    cmd = ['rsync', '-av', '--progress', '--ignore-existing'] + exclude_args + [f"{source_dir}/", str(target_dir)]
    log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)
    return True

def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...), base_data_dir: str = typer.Option(...), bids_data_dir: str = typer.Option(...)):
    source_dir = Path(bids_data_dir) / rid
    target_dir = Path(base_data_dir) / "BIDS_data" / rid
    log.info(f"Syncing original BIDS data from: {source_dir} to: {target_dir}")
    rsync_data(source_dir=source_dir, target_dir=target_dir)

#%%

if __name__ == "__main__":
    data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"
    id_map = pd.read_csv(Path(data_dir) / "id_map.csv")
    for index,subject in id_map.iterrows():
        log.info(f"Processing dataset: {subject['RID']}")
        main(rid=subject['RID'], base_data_dir=data_dir, bids_data_dir=bids_dir)