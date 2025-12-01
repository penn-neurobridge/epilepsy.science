from pathlib import Path
import logging
import shutil
import typer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


#%%

def copy_voxtool_ct(source_dir: Path, target_dir: Path):
    """
    Copy voxtool_ct derivative to Pennsieve.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    try:
        voxtool_ct = source_dir.rglob("**/ieeg/*electrodes.txt").__next__()
        log.info(f"Found voxtool_ct at: {voxtool_ct}")
    except StopIteration:
        log.error(f"Could not find voxtool_ct in {source_dir}")
        return False
    
    # Create the derivatives directory (parent directory)
    derivatives_dir = target_dir / 'derivatives'
    derivatives_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Ensured derivatives directory exists: {derivatives_dir}")

    target_voxtool_ct_dir = derivatives_dir / 'voxtool_ct'
    target_voxtool_ct_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"syncing to: {target_voxtool_ct_dir}")
    # copy voxtool_ct to target_voxtool_ct_dir with name electrodes.txt
    shutil.copy(voxtool_ct, target_voxtool_ct_dir / "electrodes.txt")
    return True

def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...), base_data_dir: str = typer.Option(...), bids_data_dir: str = typer.Option(...)):
    source_dir = Path(bids_data_dir) / rid
    target_dir = Path(base_data_dir) / "output" / pennepi
    log.info(f"Syncing voxtool_ct from: {source_dir} to: {target_dir}")
    copy_voxtool_ct(source_dir=source_dir, target_dir=target_dir)

#%%

if __name__ == "__main__":
    data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"
    rid = "sub-RID0018"
    pennepi = "PennEPI00159"
    main(rid=rid, pennepi=pennepi, base_data_dir=data_dir, bids_data_dir=bids_dir)
