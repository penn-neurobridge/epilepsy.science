#%%
import subprocess
import logging
import typer
import pandas as pd
from io import StringIO

from pathlib import Path


# Configure logging to show info messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

#%%
def diff_dataset(dataset_name, base_data_dir="data"):
    """
    Check if Pennsieve dataset has changed between local and remote.
    
    Args:
        dataset_name (str): The name of the dataset (used for directory name)
        base_data_dir (str): Base directory where datasets will be stored (default: "data")
    
    Returns:
        pd.DataFrame or None: DataFrame with changed files, or None if error occurred
    """
    # Create the full path for the dataset directory
    dataset_path = Path(base_data_dir) / "output" / dataset_name
    
    try:
        # Ensure the base data directory exists
        data_dir = Path(base_data_dir)
        data_dir.mkdir(exist_ok=True)
        
        log.info(f"Checking if '{dataset_name}' has changed between local and remote")
        # Run the pennsieve map command
        result = subprocess.run([
            'pennsieve', 
            'map',
            'diff', 
            str(dataset_path)
        ], capture_output=True, text=True, check=True)
        log.info(result.stdout)

        # Parse table from stdout manually
        lines = result.stdout.strip().split('\n')
        
        # Find header row and data rows
        header_row = None
        data_rows = []
        found_header = False
        
        for line in lines:
            line = line.strip()
            
            # Skip separator lines (lines that start with + or are empty)
            if not line or line.startswith('+'):
                continue
            
            # Check if this is the header row (contains both PATH and FILE NAME)
            if 'FILE NAME' in line.upper() and 'PATH' in line.upper():
                header_row = line
                found_header = True
            elif found_header and '|' in line:
                # This is a data row (has pipes but is not a header)
                if 'FILE NAME' not in line.upper():
                    data_rows.append(line)
        
        if not header_row:
            log.warning("Could not find header row in diff output")
            return pd.DataFrame()
        
        # Parse header row - split by pipe and clean
        # Keep empty cells but remove leading/trailing pipes
        header_parts = header_row.split('|')
        headers = [col.strip() for col in header_parts[1:-1]]  # Skip first and last (empty due to leading/trailing pipes)
        
        # Parse data rows
        rows = []
        for row_line in data_rows:
            # Split by pipe and keep ALL cells (including empty ones)
            # Remove first and last elements (empty due to leading/trailing pipes)
            row_parts = row_line.split('|')
            cells = [cell.strip() for cell in row_parts[1:-1]]
            # Only add rows that have the same number of columns as headers
            if len(cells) == len(headers):
                rows.append(cells)
        
        # Create DataFrame
        if not rows:
            log.info("No changes detected")
            return pd.DataFrame(columns=headers)
        
        df = pd.DataFrame(rows, columns=headers)
        
        # Reset index after filtering
        df = df.reset_index(drop=True)
        
        # Find column names (they might vary slightly)
        file_name_col = None
        path_col = None
        update_col = None
        for col in df.columns:
            if 'FILE NAME' in col.upper() or 'FILENAME' in col.upper():
                file_name_col = col
            if 'PATH' in col.upper() and 'FULL' not in col.upper():
                path_col = col
            if 'UPDATE' in col.upper() or 'CHANGE' in col.upper():
                update_col = col
        
        # Add full path column for easier use
        base = Path(dataset_path)
        if file_name_col:
            if path_col:
                df['FULL_PATH'] = df.apply(
                    lambda row: str(base / str(row[path_col]).strip() / row[file_name_col]) 
                    if pd.notna(row[path_col]) and str(row[path_col]).strip() 
                    else str(base / row[file_name_col]),
                    axis=1
                )
            else:
                df['FULL_PATH'] = df[file_name_col].apply(lambda x: str(base / x))

        return df
        
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to diff '{dataset_name}': {e.stderr}")
        return None
        
    except FileNotFoundError:
        log.error("The 'pennsieve' command was not found. Make sure Pennsieve agent is running.")
        return None
        
    except Exception as e:
        log.error(f"Error diffing '{dataset_name}': {e}")
        import traceback
        log.error(traceback.format_exc())
        return None

# %%
def main(
    base_data_dir: str = typer.Option("data", help="The directory where the datasets are mapped"),
    dataset_name: str = typer.Option(..., help="The name of the dataset to diff"),
    output_csv: str = typer.Option(None, "--output-csv", "-o", help="Optional: Save results to CSV file")
):
    """
    Check if Pennsieve datasets have changed between local and remote.
    
    Args:
        base_data_dir: The directory where the datasets are mapped
        dataset_name: The name of the dataset to diff
        output_csv: Optional path to save results as CSV
    """
    
    # Diff the datasets
    df = diff_dataset(dataset_name, base_data_dir)
    
    # Check if we got a valid DataFrame
    if df is None:
        log.error("Failed to get diff results")
        return
    
    if len(df) == 0:
        log.info("No changes detected between local and remote")
        return
    
    # Save to CSV if requested
    if output_csv:
        df.to_csv(output_csv, index=False)
        log.info(f"\nResults saved to: {output_csv}")

    return df
# %%
if __name__ == "__main__":
    # Use typer for command-line arguments
    typer.run(main)