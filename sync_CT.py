from pathlib import Path
import logging
import shutil
import typer
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


#%%

def copy_ct(source_dir: Path, target_dir: Path):
    """
    Copy CT derivative to Pennsieve.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    try:
        ct_file = source_dir.rglob("**/ct/*.nii.gz").__next__()
        log.info(f"Found CT at: {ct_file}")
    except StopIteration:
        log.error(f"Could not find CT in {source_dir}")
        return False
    
    # Create the derivatives directory (parent directory)
    derivatives_dir = target_dir.rglob("ses-postimplant").__next__()

    target_voxtool_ct_file = derivatives_dir / 'ct'/ f"{derivatives_dir.parent.name}_ses-postimplant_acq-3D_defaced_ct.nii.gz"
    target_voxtool_ct_file.parent.mkdir(parents=True, exist_ok=True)

    if not target_voxtool_ct_file.exists():
        log.info(f"Syncing CT to: {target_voxtool_ct_file}")
        cmd = ['fslmaths', str(ct_file), '-thr', '0', str(target_voxtool_ct_file)]
        log.info(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        log.info(f"Synced CT to: {target_voxtool_ct_file}")
    else:
        log.info(f"Target file already exists, skipping sync: {target_voxtool_ct_file}")
    return True
    
def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...), base_data_dir: str = typer.Option(...), bids_data_dir: str = typer.Option(...)):
    source_dir = Path(bids_data_dir) / rid
    target_dir = Path(base_data_dir) / "output" / pennepi
    log.info(f"Syncing voxtool_ct from: {source_dir} to: {target_dir}")
    copy_ct(source_dir=source_dir, target_dir=target_dir)

#%%

if __name__ == "__main__":
    data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"
    rid = "sub-RID0835"
    pennepi = "PennEPI00083"
    main(rid=rid, pennepi=pennepi, base_data_dir=data_dir, bids_data_dir=bids_dir)
