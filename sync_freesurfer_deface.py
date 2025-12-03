from pathlib import Path
import logging
import subprocess
import typer
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%

def deface_freesurfer(source_dir: Path, target_dir: Path):
    """
    Deface freesurfer T1.mgz at the source, then copy the defaced version to target.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    
    # Find the freesurfer directory with T1.mgz
    try:
        source_t1_file = source_dir.rglob("freesurfer/mri/T1.mgz").__next__()
        log.info(f"Found freesurfer T1 at: {source_t1_file}")
    except StopIteration:
        log.error(f"Could not find freesurfer/mri/T1.mgz in {source_dir}")
        return False
    
    # Get the source mri directory (where T1.mgz lives)
    source_mri_dir = source_t1_file.parent
    source_defaced_file = source_mri_dir / 'T1_defaced.mgz'

    if not source_defaced_file.exists():
        log.info(f"Defacing {source_t1_file} to {source_defaced_file}")
        # Run the deface command at the SOURCE where all necessary files exist
        # (T1.mgz, transforms/talairach.xfm, etc.)
        cmd = ['mideface', '--i', str(source_t1_file), 
            '--o', str(source_defaced_file), 
            '--threads', '16']
        log.info(f"Running mideface at source: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    
    # Now copy the defaced output to target directory
    target_derivatives_dir = target_dir / 'derivatives' / 'freesurfer' / 'mri'
    target_derivatives_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Created target directory: {target_derivatives_dir}")
    
    # Define target file paths
    target_t1_file = target_derivatives_dir / 'T1.mgz'
    target_t1_nii = target_derivatives_dir / 'T1.nii.gz'
    
    # Only copy and convert if target files don't exist
    if not target_t1_file.exists() or not target_t1_nii.exists():
        # Copy the defaced file to target as T1.mgz (replacing with defaced version)
        shutil.copy(source_defaced_file, target_t1_file)
        log.info(f"Copied defaced file as T1.mgz to: {target_t1_file}")

        # mri_convert the defaced file to nii.gz
        cmd = ['mri_convert', str(target_t1_file), str(target_t1_file.with_suffix('.nii.gz'))]
        log.info(f"Running mri_convert: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    else:
        log.info(f"Target files already exist, skipping copy and conversion")

    return True

def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...), base_data_dir: str = typer.Option(...), bids_data_dir: str = typer.Option(...)):
    source_dir = Path(bids_data_dir) / rid
    target_dir = Path(base_data_dir) / "output" / pennepi
    log.info(f"Syncing freesurfer from: {source_dir} to: {target_dir}")
    deface_freesurfer(source_dir=source_dir, target_dir=target_dir)
#%%

if __name__ == "__main__":
    data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"
    rid = "sub-RID0835"
    pennepi = "PennEPI00083"
    main(rid=rid, pennepi=pennepi, base_data_dir=data_dir, bids_data_dir=bids_dir)