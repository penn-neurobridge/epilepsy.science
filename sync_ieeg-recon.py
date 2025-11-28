from pathlib import Path
import logging
import subprocess
import typer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


#%%

def rsync_ieeg_recon(source_dir: Path, target_dir: Path):
    """
    Copy ieeg_recon derivative to Pennsieve.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
    """
    try:
        ieeg_recon_dir = source_dir.rglob("ieeg_recon").__next__()
        log.info(f"Found ieeg_recon at: {ieeg_recon_dir}")
    except StopIteration:
        log.error(f"Could not find ieeg_recon in {source_dir}")
        return False
    
    # Create the derivatives directory (parent directory)
    derivatives_dir = target_dir / 'derivatives'
    derivatives_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Ensured derivatives directory exists: {derivatives_dir}")

    target_ieeg_recon_dir = derivatives_dir / 'ieeg_recon'

    log.info(f"syncing to: {target_ieeg_recon_dir}")
    cmd = ['rsync', '-av', '--progress', f"{ieeg_recon_dir}/", str(target_ieeg_recon_dir)]
    log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=True)
    return True

def main(rid : str = typer.Option(...), pennepi : str = typer.Option(...)):
    source_dir = Path('/app/data/BIDS') / rid
    target_dir = Path(__file__).parent / "data" / "output" / pennepi
    log.info(f"Syncing ieeg_recon from: {source_dir} to: {target_dir}")
    rsync_ieeg_recon(source_dir=source_dir, target_dir=target_dir)

#%%

if __name__ == "__main__":
    typer.run(main)
