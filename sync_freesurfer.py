from pathlib import Path
import logging
import subprocess
import typer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# List of freesurfer files to include. Other may have PHI so do not include them.
FREESURFER_FILES = [
    "label/lh.aparc.DKTatlas.annot",
    "label/lh.aparc.a2009s.annot",
    "label/lh.aparc.annot",
    "label/lh.lausanne2018.scale1.annot",
    "label/lh.lausanne2018.scale2.annot",
    "label/lh.lausanne2018.scale3.annot",
    "label/lh.lausanne2018.scale4.annot",
    "label/lh.lausanne2018.scale5.annot",
    "label/rh.aparc.DKTatlas.annot",
    "label/rh.aparc.a2009s.annot",
    "label/rh.aparc.annot",
    "label/rh.lausanne2018.scale1.annot",
    "label/rh.lausanne2018.scale2.annot",
    "label/rh.lausanne2018.scale3.annot",
    "label/rh.lausanne2018.scale4.annot",
    "label/rh.lausanne2018.scale5.annot",
    "mri/antsdn.brain.mgz",
    "mri/aparc+aseg.mgz",
    "mri/aparc+aseg.nii.gz",
    "mri/aparc.DKTatlas+aseg.mgz",
    "mri/aparc.a2009s+aseg.mgz",
    "mri/aparc.a2009s+aseg.nii.gz",
    "mri/aseg.auto.mgz",
    "mri/aseg.auto_noCCseg.mgz",
    "mri/aseg.mgz",
    "mri/aseg.nii.gz",
    "mri/aseg.presurf.hypos.mgz",
    "mri/aseg.presurf.mgz",
    "mri/brain.finalsurfs.mgz",
    "mri/brain.mgz",
    "mri/brain.nii.gz",
    "mri/brainmask.auto.mgz",
    "mri/brainmask.mgz",
    "mri/filled.mgz",
    "mri/lausanne2018.scale1.mgz",
    "mri/lausanne2018.scale1.nii.gz",
    "mri/lausanne2018.scale2.mgz",
    "mri/lausanne2018.scale2.nii.gz",
    "mri/lausanne2018.scale3.mgz",
    "mri/lausanne2018.scale3.nii.gz",
    "mri/lausanne2018.scale4.mgz",
    "mri/lausanne2018.scale4.nii.gz",
    "mri/lausanne2018.scale5.mgz",
    "mri/lausanne2018.scale5.nii.gz",
    "mri/lh.ribbon.mgz",
    "mri/norm.mgz",
    "mri/rh.ribbon.mgz",
    "mri/ribbon.mgz",
    "mri/talairach.log",
    "mri/talairach_with_skull.log",
    "mri/transforms/talairach.xfm",
    "mri/wm.asegedit.mgz",
    "mri/wm.mgz",
    "mri/wm.nii.gz",
    "mri/wm.seg.mgz",
    "mri/wmparc.mgz",
    "surf/lh.inflated",
    "surf/lh.pial",
    "surf/lh.white",
    "surf/rh.inflated",
    "surf/rh.pial",
    "surf/rh.white",
]

FREESURFER_DEFACE_FILES = [
    "mri/T1.mgz"
]

#%%

def rsync_freesurfer(source_dir: Path, target_dir: Path):
    """
    Rsync freesurfer derivative to Pennsieve.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    
    # Find the freesurfer directory
    try:
        freesurfer_dir = source_dir.rglob("freesurfer").__next__()
        log.info(f"Found freesurfer at: {freesurfer_dir}")
    except StopIteration:
        log.error(f"Could not find freesurfer in {source_dir}")
        return False
    
    # Create the derivatives directory (parent directory)
    derivatives_dir = target_dir / 'derivatives'
    derivatives_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Ensured derivatives directory exists: {derivatives_dir}")

    target_freesurfer_dir = derivatives_dir / 'freesurfer'

    log.info(f"syncing to: {target_freesurfer_dir}")
    
    # Build the rsync command with includes/excludes
    # Start with base command
    cmd = ['rsync', '-av', '--progress', '--ignore-existing']
    
    # Exclude .DS_Store files
    cmd.extend(['--exclude=.DS_Store'])
    
    # Include only the directories we need (label, mri, surf) and their parent structure
    cmd.extend(['--include=label/', '--include=mri/', '--include=mri/transforms/', '--include=surf/'])
    
    # Include each specific file from our list
    for file_path in FREESURFER_FILES:
        cmd.extend(['--include', file_path])
    
    # Exclude everything else
    cmd.extend(['--exclude=*'])
    
    # Add source and destination
    cmd.extend([f"{freesurfer_dir}/", str(target_freesurfer_dir)])
    
    log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)
    return True

def deface_freesurferT1(source_dir: Path, target_dir: Path):
    """
    Deface the freesurfer T1 image.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    try:
        freesurfer_T1_file = source_dir.rglob("freesurfer/mri/T1.mgz").__next__()
        log.info(f"Found freesurfer T1 at: {freesurfer_T1_file}")
    except StopIteration:
        log.error(f"Could not find freesurfer T1 in {source_dir}")
        return False
    
    target_freesurfer_T1_file = target_dir / 'derivatives' / 'freesurfer' / 'mri' / 'T1.mgz'
    target_freesurfer_T1_file_nii = target_dir / 'derivatives' / 'freesurfer' / 'mri' / 'T1.nii.gz'
    target_freesurfer_T1_file.mkdir(parents=True, exist_ok=True)

    cmd = ['mideface', '--i', str(freesurfer_T1_file), '--o', str(target_freesurfer_T1_file)]
    log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)

    # mri convert to nii.gz
    cmd = ['mri_convert', str(target_freesurfer_T1_file), str(target_freesurfer_T1_file_nii)]
    log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)

    return True

def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...), base_data_dir: str = typer.Option(...), bids_data_dir: str = typer.Option(...)):
    source_dir = Path(bids_data_dir) / rid
    target_dir = Path(base_data_dir) / "output" / pennepi
    log.info(f"Syncing freesurfer from: {source_dir} to: {target_dir}")
    rsync_freesurfer(source_dir=source_dir, target_dir=target_dir)
    # check if freesurfer T1 exists in the target directory
    if not (target_dir / 'derivatives' / 'freesurfer' / 'mri' / 'T1.nii.gz').exists():
        log.info(f"Defacing freesurfer T1 from: {source_dir} to: {target_dir}")
        deface_freesurferT1(source_dir=source_dir, target_dir=target_dir)
    else:
        log.info(f"Freesurfer T1 already exists in {target_dir}")
#%%

if __name__ == "__main__":
    data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"
    rid = "sub-RID0018"
    pennepi = "PennEPI00159"
    main(rid=rid, pennepi=pennepi, base_data_dir=data_dir, bids_data_dir=bids_dir)