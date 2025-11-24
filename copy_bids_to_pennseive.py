import shutil
from pathlib import Path
import logging
import typer
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

app = typer.Typer(help="Copy BIDS derivatives to Pennsieve output directories")

#%%

def copy_ieeg_recon(source_dir: Path, target_dir: Path, name_derivative: str):
    """
    Copy ieeg_recon derivative to Pennsieve.
    
    Args:
        source_dir (Path): The source directory where the data is stored
        target_dir (Path): The target directory where the data will be copied to
        name_derivative (str): The name of the derivative to copy
    """
    try:
        ieeg_recon_dir = source_dir.rglob(f'{name_derivative}').__next__()
        log.info(f"Found {name_derivative} at: {ieeg_recon_dir}")
    except StopIteration:
        log.error(f"Could not find '{name_derivative}' in {source_dir}")
        raise typer.Exit(code=1)
    
    target_ieeg_recon_dir = target_dir / 'derivatives' / 'ieeg_recon'
    
    if target_ieeg_recon_dir.exists():
        log.warning(f"Target directory already exists: {target_ieeg_recon_dir}")
    
    log.info(f"Copying to: {target_ieeg_recon_dir}")
    shutil.copytree(ieeg_recon_dir, target_ieeg_recon_dir, dirs_exist_ok=True)
    log.info(f"✓ Successfully copied {name_derivative}")



#%%

@app.command()
def main(
    source_dir: str = typer.Argument(
        ...,
        help="Source BIDS directory containing the subject data (e.g., /path/to/BIDS/sub-RID0054)"
    ),
    target_dir: str = typer.Argument(
        ...,
        help="Target Pennsieve directory (e.g., data/output/PennEPI00143)"
    ),
    name_derivative: str = typer.Option(
        "ieeg_recon",
        "--derivative", "-d",
        help="Name of the derivative to copy (e.g., ieeg_recon)"
    )
):
    """
    Copy BIDS derivatives to Pennsieve output directories.
    
    Example:
        uv run python copy_bids_to_pennseive.py \\
            /path/to/BIDS/sub-RID0054 \\
            data/output/PennEPI00143 \\
            --derivative ieeg_recon
    """
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    
    # Validate paths
    if not source_path.exists():
        log.error(f"Source directory does not exist: {source_path}")
        raise typer.Exit(code=1)
    
    if not target_path.exists():
        log.info(f"Target directory does not exist, creating: {target_path}")
        target_path.mkdir(parents=True, exist_ok=True)
    
    log.info(f"Copying {name_derivative} from {source_path} to {target_path}")

    if name_derivative == "ieeg_recon":
        copy_ieeg_recon(source_path, target_path, name_derivative)
    else:
        log.error(f"Unknown derivative: {name_derivative}")
        log.info("Currently supported derivatives: ieeg_recon")
        raise typer.Exit(code=1)
    
    log.info(f"✓ Done copying {name_derivative}")

#%%

if __name__ == "__main__":
    app()
