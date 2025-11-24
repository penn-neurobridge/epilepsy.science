import re
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def extract_rids_from_file(recon_file="data/id_recon.txt"):
    """
    Extract RIDs from the id_recon.txt file.
    
    Each line contains a path like:
    /path/to/sub-RID0018/derivatives/ieeg_recon/module4
    
    Args:
        recon_file: Path to id_recon.txt
        
    Returns:
        set: Set of RID strings (e.g., {'sub-RID0018', 'sub-RID0020', ...})
    """
    rids = set()
    
    try:
        with open(recon_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Split by / and find the part that starts with 'sub-RID'
                parts = line.split('/')
                for part in parts:
                    if part.startswith('sub-RID'):
                        rids.add(part)
                        break
        
        log.info(f"Extracted {len(rids)} unique RIDs from {recon_file}")
        return rids
        
    except FileNotFoundError:
        log.warning(f"File not found: {recon_file}")
        return set()


def main(pennepi_csv="data/mastersubject_metadata.csv", 
         hup_rid="data/hup_rid.csv",
         recon_file="data/id_recon.txt",
         output_csv="data/id_map.csv",
         filter_by_recon=True):
    """
    Create ID mapping and optionally filter by RIDs in id_recon.txt.
    
    Args:
        pennepi_csv: Path to mastersubject_metadata.csv
        hup_rid: Path to hup_rid.csv
        recon_file: Path to id_recon.txt (contains RIDs with ieeg_recon)
        output_csv: Path to save output mapping
        filter_by_recon: If True, only include RIDs present in id_recon.txt
        
    Returns:
        pd.DataFrame: ID mapping (optionally filtered)
    """
    # Load master metadata
    pennepi_df = pd.read_csv(pennepi_csv, low_memory=False)

    # Load HUP-RID mapping
    hup_rid_df = pd.read_csv(hup_rid)
    # Format hupsubjno as HUP### (3 digits) - convert float to int first
    hup_rid_df['hupsubjno_formatted'] = 'HUP' + hup_rid_df['hupsubjno'].fillna(0).astype(int).astype(str).str.zfill(3)
    # Format record_id as sub-RID#### (4 digits)
    hup_rid_df['record_id_formatted'] = 'sub-RID' + hup_rid_df['record_id'].astype(int).astype(str).str.zfill(4)
    
    # Select only the required columns from PennEPI data
    result_df = pennepi_df[[
        'HUPnumber',
        'participant_id',
        'priority',
        'expect_recon'
    ]].copy()
    
    # Normalize HUPnumber to HUP### format
    # Extract just the numbers and convert to HUP### format
    def normalize_hup(hup_val):
        if pd.isna(hup_val):
            return None
        hup_str = str(hup_val)

        # Extract digits
        digits = re.findall(r'\d+', hup_str)
        if digits:
            # Take the first number found and format as HUP###
            return 'HUP' + digits[0].zfill(3)
        return None
    
    result_df['HUPnumber_normalized'] = result_df['HUPnumber'].apply(normalize_hup)
    
    # Merge with HUP-RID mapping to add RID column
    result_df = result_df.merge(
        hup_rid_df[['hupsubjno_formatted', 'record_id_formatted']], 
        left_on='HUPnumber_normalized', 
        right_on='hupsubjno_formatted', 
        how='left'
    )
    
    # Rename columns for clarity
    result_df = result_df.rename(columns={
        'record_id_formatted': 'RID',
        'hupsubjno_formatted': 'HUP',
        'participant_id': 'PennEPI'
    })
    
    # Keep only the columns we need in the desired order
    result_df = result_df[['RID', 'HUP', 'PennEPI', 'priority', 'expect_recon']].copy()
    
    log.info(f"Total datasets before filtering: {len(result_df)}")
    
    # Filter by RIDs in id_recon.txt if requested
    if filter_by_recon:
        recon_rids = extract_rids_from_file(recon_file)
        
        if recon_rids:
            # Filter to only include RIDs that are in the recon file
            result_df = result_df[result_df['RID'].isin(recon_rids)].copy()
            log.info(f"Datasets after filtering by id_recon.txt: {len(result_df)}")
        else:
            log.warning("No RIDs found in recon file, skipping filter")
    
    # Sort by RID for easy viewing
    result_df = result_df.sort_values('RID').reset_index(drop=True)
    
    # Save to CSV
    result_df.to_csv(output_csv, index=False)
    log.info(f"Saved ID mapping to: {output_csv}")
    
    # Print summary
    log.info(f"\nSummary:")
    log.info(f"  Total datasets: {len(result_df)}")
    log.info(f"  With RID: {result_df['RID'].notna().sum()}")
    log.info(f"  With HUP: {result_df['HUP'].notna().sum()}")
    log.info(f"  With PennEPI: {result_df['PennEPI'].notna().sum()}")
    log.info(f"  With expect_recon: {result_df['expect_recon'].notna().sum()}")
    
    # Print first few rows
    print("\nFirst 15 rows:")
    print(result_df.head(15).to_string(index=False))
    
    return result_df


if __name__ == "__main__":
    main()

