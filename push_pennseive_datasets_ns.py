"""
Push Pennsieve Datasets - Upload ADDED files to Pennsieve

This script uploads ONLY newly ADDED files from locally mapped datasets to Pennsieve.
Uses Pennsieve manifest-based upload with proper target path mapping (-t flag).

Key Features:
    - Only uploads files marked as "ADDED" in diff (safe, no overwrites)
    - Each file uploaded with correct target path on Pennsieve
    - Dry-run mode to preview changes
    - Batch processing for multiple datasets

Prerequisites:
    1. Pennsieve Agent running: pennsieve agent
    2. Datasets mapped locally: map_pennseive_datasets.py
    3. Valid Pennsieve credentials configured

Workflow:
    1. Run diff to identify ADDED files
    2. Set active dataset
    3. Create manifest with first file + target path (-t flag)
    4. Add remaining files to manifest with target paths
    5. Upload manifest

Usage:
    # Push single dataset (only ADDED files)
    uv run push_pennseive_datasets.py -n "PennEPI00143"
    
    # Push multiple datasets
    uv run push_pennseive_datasets.py -n "PennEPI00143,PennEPI00049"
    
    # Dry run (preview what will be uploaded)
    uv run push_pennseive_datasets.py -n "PennEPI00143" --dry-run

Reference:
    https://docs.pennsieve.io/docs/uploading-files-using-the-pennsieve-agent
"""
#%%
import subprocess
import logging
import re
import typer
import pandas as pd
import get_pennseive_datasets as pennseive
import diff_pennseive_datasets as diff_pennseive

from pathlib import Path


# Configure logging to show info messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%
def push_dataset(dataset_id, dataset_name, base_data_dir="data", upload_path=None, dry_run=False, diff_df=None):
    """
    Push ADDED files from a locally mapped dataset to Pennsieve.
    
    Workflow:
        1. Set active dataset
        2. Identify ADDED files from diff
        3. Create manifest with first file + target path (-t flag)
        4. Add remaining files to manifest with target paths (-t flag)
        5. Upload manifest
    
    Args:
        dataset_id (str): Pennsieve dataset ID (format: N:dataset:xxx)
        dataset_name (str): Dataset name (used for directory)
        base_data_dir (str): Base directory where datasets are mapped (default: "data")
        upload_path (str): Unused (kept for compatibility)
        dry_run (bool): If True, preview without uploading
        diff_df (pd.DataFrame): Diff results. If None, runs diff automatically
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Create the full path for the dataset directory
    dataset_path = Path(base_data_dir) / "output" / dataset_name
    
    if not dataset_path.exists():
        log.error(f"Dataset directory does not exist: {dataset_path}")
        log.info(f"Hint: You may need to map the dataset first using map_pennseive_datasets.py")
        return False
    
    try:
        # Step 1: Set the active dataset
        log.info(f"Setting active dataset to '{dataset_name}' (ID: {dataset_id})")
        result = subprocess.run([
            'pennsieve',
            'dataset',
            'use',
            dataset_id
        ], check=True)
        
        # Step 2: Get diff to identify ADDED files
        if diff_df is None:
            log.info("Running diff to identify files to upload...")
            diff_df = diff_pennseive.diff_dataset(
                dataset_name=dataset_name,
                base_data_dir=base_data_dir
            )
            
            if diff_df is None:
                log.error("Failed to get diff results")
                return False
        
        # Filter for ADDED files only
        # Find the UPDATE/CHANGE column
        update_col = None
        for col in diff_df.columns:
            if 'UPDATE' in col.upper() or 'CHANGE' in col.upper():
                update_col = col
                break
        
        if update_col is None:
            log.error("Could not find UPDATE column in diff results")
            log.info(f"Available columns: {diff_df.columns.tolist()}")
            return False
        
        # Filter for ADDED files
        added_files = diff_df[diff_df[update_col].str.upper() == 'ADDED'].copy()
        
        if len(added_files) == 0:
            log.info("No ADDED files found. Nothing to upload.")
            return True
        
        log.info(f"Found {len(added_files)} ADDED files to upload")
        
        if dry_run:
            log.info("DRY RUN: Would upload the following files:")
            for idx, row in added_files.iterrows():
                file_name = row.get('FILE NAME', row.get('File Name', ''))
                path = row.get('PATH', row.get('Path', ''))
                log.info(f"  - {path}/{file_name}")
            return True
        
        # Get column names
        file_name_col = None
        path_col = None
        full_path_col = None
        
        for col in added_files.columns:
            if 'FILE NAME' in col.upper() or 'FILENAME' in col.upper():
                file_name_col = col
            if 'PATH' in col.upper() and 'FULL' not in col.upper():
                path_col = col
            if 'FULL_PATH' in col.upper() or 'FULL PATH' in col.upper():
                full_path_col = col
        
        if not file_name_col or not path_col:
            log.error("Could not find required columns in diff results")
            log.info(f"Available columns: {added_files.columns.tolist()}")
            return False
        
        # Step 3: Create manifest with first file
        # Note: pennsieve manifest create requires a file path, not just --dataset flag
        first_file = added_files.iloc[0]
        first_full_path = first_file.get(full_path_col, '') if full_path_col else ""
        first_target_path = first_file[path_col] if not pd.isna(first_file[path_col]) else ""
        first_file_name = first_file[file_name_col]
        
        # If we don't have FULL_PATH, construct it
        if not first_full_path:
            first_full_path = str(dataset_path / str(first_target_path).strip() / first_file_name) if first_target_path else str(dataset_path / first_file_name)
        
        if not Path(first_full_path).exists():
            log.error(f"First file not found: {first_full_path}")
            return False
        
        log.info(f"Creating manifest with first file: {first_file_name} -> {first_target_path}")
        
        # Build create command with target path flag
        create_cmd = [
            'pennsieve',
            'manifest',
            'create',
            str(first_full_path)
        ]
        
        # Add target path if specified
        if first_target_path and str(first_target_path).strip():
            create_cmd.extend(['-t', str(first_target_path).strip()])
        
        # log the create command
        log.info(f"Create command: {create_cmd}")

        result = subprocess.run(create_cmd, capture_output=True, text=True, check=True)
        
        log.info(result.stdout)
        
        # Extract manifest ID from output
        # Expected format: "Manifest ID: 40 Message: Successfully indexed 1 files."
        manifest_id = None
        for line in result.stdout.split('\n'):
            # Look for "ID:" or "Manifest ID:" pattern
            if 'id' in line.lower():
                # Extract first number after "ID:"
                match = re.search(r'ID:\s*(\d+)', line, re.IGNORECASE)
                if match:
                    manifest_id = match.group(1)
                    break
        
        if not manifest_id:
            log.error("Could not extract manifest ID from output.")
            log.error(f"Output was: {result.stdout}")
            return False
        
        log.info(f"Created manifest with ID: {manifest_id}")
        
        # Step 4: Add remaining ADDED files to the manifest with proper target path
        # (First file already added during manifest creation)
        remaining_files = added_files.iloc[1:]  # Skip first file
        
        if len(remaining_files) > 0:
            log.info(f"Adding {len(remaining_files)} more files to manifest {manifest_id}...")
        else:
            log.info("Only one file to upload (already in manifest)")
        
        # Add remaining files one by one with target path
        success_count = 1  # First file already added
        failed_count = 0
        
        for idx, row in remaining_files.iterrows():
            file_name = row[file_name_col]
            target_path = row[path_col] if not pd.isna(row[path_col]) else ""
            local_full_path = row.get(full_path_col, '') if full_path_col else ""
            
            # If we don't have FULL_PATH, construct it
            if not local_full_path:
                local_full_path = str(dataset_path / str(target_path).strip() / file_name) if target_path else str(dataset_path / file_name)
            
            # Check if file exists
            if not Path(local_full_path).exists():
                log.warning(f"File not found, skipping: {local_full_path}")
                failed_count += 1
                continue
            
            # Add file to manifest with target path
            log.info(f"Adding: {file_name} -> {target_path}")
            
            add_cmd = [
                'pennsieve',
                'manifest',
                'add',
                manifest_id,
                str(local_full_path)
            ]
            
            # Add target path if specified
            if target_path and str(target_path).strip():
                add_cmd.extend(['-t', str(target_path).strip()])
            
            try:
                result = subprocess.run(add_cmd, capture_output=True, text=True, check=True)
                success_count += 1
                log.debug(f"Added successfully: {file_name}")
            except subprocess.CalledProcessError as e:
                log.error(f"Failed to add {file_name}: {e.stderr}")
                failed_count += 1
                continue
        
        log.info(f"Total files in manifest: {success_count}, failed: {failed_count}")
        
        if success_count == 0:
            log.error("No files were added to manifest successfully")
            return False
        
        # Step 5: Upload the manifest
        log.info(f"Starting upload for manifest {manifest_id}...")
        result = subprocess.run([
            'pennsieve',
            'upload',
            'manifest',
            manifest_id
        ], capture_output=True, text=True, check=True)
        log.info(result.stdout)
        
        log.info(f"✓ Upload initiated for '{dataset_name}'")
        log.info(f"Uploaded {success_count} ADDED files to manifest {manifest_id}")
        log.info(f"Monitor progress: pennsieve manifest list {manifest_id}")
        log.info(f"Or subscribe to updates: pennsieve agent subscribe")
        return True
        
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to push '{dataset_name}': {e.stderr}")
        return False
        
    except FileNotFoundError:
        log.error("The 'pennsieve' command was not found. Make sure Pennsieve agent is running.")
        log.info("Start the agent with: pennsieve agent")
        return False
        
    except Exception as e:
        log.error(f"Error pushing '{dataset_name}': {e}")
        import traceback
        log.error(traceback.format_exc())
        return False

# %%
def main(
    base_data_dir: str = typer.Option("data", help="The directory where the datasets are mapped"),
    dataset_name: str = typer.Option("", "--dataset-name", "-n", help="The name(s) of the dataset(s) to push. Can be a single string or a comma-separated list."),
    upload_path: str = typer.Option(None, "--upload-path", "-p", help="Optional: Specific file or directory path within the dataset to upload"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Show what would be uploaded without actually uploading")
    # base_data_dir, dataset_name, upload_path=None, dry_run=False
):
    """
    Push ADDED files from mapped datasets to Pennsieve.
    
    Uses diff to identify ADDED files, then uploads each with proper target path
    mapping (-t flag) to ensure correct Pennsieve directory structure.
    
    Args:
        base_data_dir: Directory where datasets are mapped
        dataset_name: Dataset name(s) to push (comma-separated for multiple)
        upload_path: Unused (kept for compatibility)
        dry_run: If True, preview without uploading
    
    Examples:
        uv run push_pennseive_datasets.py -n "PennEPI00143"
        uv run push_pennseive_datasets.py -n "PennEPI00143,PennEPI00049"
        uv run push_pennseive_datasets.py -n "PennEPI00143" --dry-run
    
    Note:
        Only uploads ADDED files. Modified/deleted files are ignored.
    """
    # Get all PennEPI datasets
    log.info("Fetching Pennsieve datasets...")
    pennepi_collection = pennseive.main()

    # Filter datasets if dataset_name is provided
    if dataset_name:
        # Handle comma-separated string by splitting it
        if "," in dataset_name:
            # Split by comma and strip whitespace from each name
            filter_names = [name.strip() for name in dataset_name.split(",")]
        else:
            # Single dataset name
            filter_names = [dataset_name]
        
        # Filter the collection to only include matching datasets
        pennepi_collection = [
            dataset for dataset in pennepi_collection 
            if dataset['name'] in filter_names
        ]
        
        # Log if no datasets matched
        if not pennepi_collection:
            log.warning(f"No datasets found matching: {filter_names}")
            return
    else:
        log.error("Please specify at least one dataset name using --dataset-name or -n")
        return
    
    # Push each dataset in the collection
    success_count = 0
    failure_count = 0
    
    for dataset in pennepi_collection:
        log.info(f"Processing dataset: {dataset['name']}")
        
        # Check for differences first 
        log.info("Checking for differences between local and remote...")
        diff_df = diff_pennseive.diff_dataset(
            dataset_name=dataset['name'],
            base_data_dir=base_data_dir
        )
        
        if diff_df is None:
            log.error(f"Failed to check differences for '{dataset['name']}'. Skipping upload.")
            failure_count += 1
            continue
        
        if len(diff_df) == 0:
            log.info(f"No changes detected for '{dataset['name']}'. Skipping upload.")
            continue
        
        log.info(f"Found {len(diff_df)} changed files:")
        # Show summary of changes
        if 'UPDATE' in diff_df.columns or any('update' in col.lower() for col in diff_df.columns):
            update_col = [col for col in diff_df.columns if 'update' in col.lower()][0]
            change_summary = diff_df[update_col].value_counts()
            for change_type, count in change_summary.items():
                log.info(f"  - {change_type}: {count} files")
        
        # Push the dataset (pass diff_df so it knows which files to upload)
        success = push_dataset(
            dataset_id=dataset['id'],
            dataset_name=dataset['name'],
            base_data_dir=base_data_dir,
            upload_path=upload_path,
            dry_run=dry_run,
            diff_df=diff_df  # Pass the diff results
        )
        
        if success:
            success_count += 1
        else:
            failure_count += 1
    
    # Summary
    log.info(f"\n{'='*60}")
    log.info(f"Upload Summary:")
    log.info(f"  Successful: {success_count}")
    log.info(f"  Failed: {failure_count}")
    log.info(f"{'='*60}")
    
    if not dry_run and success_count > 0:
        log.info("\nNote: Files may still be processing on Pennsieve.")
        log.info("Use diff_pennseive_datasets.py to verify uploads are complete.")
        
# %%
if __name__ == "__main__":
    typer.run(main)
    # base_data_dir = "/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
    # dataset_name = "PennEPI00159"
    # upload_path = None
    # main(base_data_dir, dataset_name)
# %%
