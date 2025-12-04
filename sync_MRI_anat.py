from pathlib import Path
import logging
import subprocess
import typer
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%

def deface_anat(source_dir: Path, target_dir: Path):
    """
    Deface freesurfer T1.mgz at the source, then copy the defaced version to target.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    
    # Find if the patient has a research session only or both clinical and research sessions.
    # If research session is present then use the research session data.
    try:
        research_session = source_dir.rglob("**/ses-research*/**").__next__()
        log.info(f"Found research session at: {research_session}")
        source_dir = research_session
    except StopIteration:
        log.error(f"Could not find ses-research in {source_dir}")
        try:
            clinical_session = source_dir.rglob("**/ses-clinical*/**").__next__()
            log.info(f"Found clinical session at: {clinical_session}")
            source_dir = clinical_session
        except StopIteration:
            log.error(f"Could not find ses-clinical in {source_dir}")
            raise ValueError(f"Could not find any session in {source_dir}. Please check the data.")
        
    # Find anat file in the source directory
    try:
        source_t1_file = source_dir.rglob("**/anat/*T1w.nii.gz").__next__()
        log.info(f"Found T1w file at: {source_t1_file}")
    except StopIteration:
        log.error(f"Could not find T1w file in {source_dir}")
        raise ValueError(f"Could not find T1w file in {source_dir}. Please check the data.")
    
    source_t1_dir = source_t1_file.parent
    source_defaced_file = source_t1_dir / source_t1_file.name.replace('.nii.gz', '_defaced.nii.gz')

    if not source_defaced_file.exists():
        log.info(f"Defacing {source_t1_file} to {source_defaced_file}")
        # Run the deface command at the SOURCE where all necessary files exist
        # (T1.mgz, transforms/talairach.xfm, etc.)
        cmd = ['mideface', '--i', str(source_t1_file), 
            '--o', str(source_defaced_file), 
            '--threads', '16']
        log.info(f"Running mideface at source: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            log.error(f"Mideface failed with error: {e.stderr}")

    derivatives_dir = target_dir.rglob("ses-postimplant").__next__()
    derivatives_dir = derivatives_dir.parent / 'ses-preimplant'

    # Now copy the defaced output to target directory
    target_derivatives_anat_file = derivatives_dir / 'anat' / f"{derivatives_dir.parent.name}_ses-preimplant_acq-3D_defaced_T1w.nii.gz"
    target_derivatives_anat_file.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Created target directory: {target_derivatives_anat_file}")

    # Only copy and convert if target files don't exist
    if not target_derivatives_anat_file.exists():
        # Copy the defaced file to target as T1.mgz (replacing with defaced version)
        shutil.copy(source_defaced_file, target_derivatives_anat_file)
        log.info(f"Copied defaced file to: {target_derivatives_anat_file}")
    else:
        log.info(f"Target file already exists, skipping copy and conversion")

    return True

def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...), base_data_dir: str = typer.Option(...), bids_data_dir: str = typer.Option(...)):
    source_dir = Path(bids_data_dir) / rid
    target_dir = Path(base_data_dir) / "output" / pennepi
    log.info(f"Syncing anat from: {source_dir} to: {target_dir}")
    deface_anat(source_dir=source_dir, target_dir=target_dir)
#%%

if __name__ == "__main__":
    data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"
    rid = "sub-RID0835"
    pennepi = "PennEPI00083"
    main(rid=rid, pennepi=pennepi, base_data_dir=data_dir, bids_data_dir=bids_dir)