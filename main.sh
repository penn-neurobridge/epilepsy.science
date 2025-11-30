#!/bin/bash
# 
# Pennsieve Dataset Curation Pipeline
# This script demonstrates the complete workflow for curating epilepsy.science datasets
#

echo "Starting Pennsieve Dataset Curation Pipeline..."

# Start the Pennsieve agent
echo "Starting Pennsieve agent..."
pennsieve agent

# Verify agent is running with profile
echo "Verifying Pennsieve credentials..."
pennsieve whoami

# # Get all epilepsy.science datasets from Pennsieve (optional - for reference)
# echo "Fetching available datasets..."
# uv run get_pennseive_datasets.py

# Loop through all PennEPI datasets from the CSV
echo "Processing all PennEPI datasets..."

data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"

# Read the CSV file and loop through each PennEPI value
# Skip the header line (first line) and skip empty PennEPI values
tail -n +2 data/id_map.csv | head -n 1 | while IFS=',' read -r rid hup pennepi priority expect_recon; do
    # Skip if PennEPI column is empty or just says "PennEPI"
    if [[ -n "$pennepi" && "$pennepi" != "PennEPI" ]]; then
        echo "Processing dataset: $pennepi (from $rid)"
        uv run map_pennseive_datasets.py --base-data-dir $data_dir --dataset-name "$pennepi"
        uv run sync_ieeg-recon.py --rid "$rid" --pennepi "$pennepi" --base-data-dir $data_dir --bids-data-dir $bids_dir
        uv run diff_pennseive_datasets.py --dataset-name "$pennepi" --base-data-dir $data_dir
        # echo $pennepi
        # echo $data_dir
        # # set -x  # Enable command tracing
        # uv run push_pennseive_datasets.py --dataset-name "$pennepi" --base-data-dir "$data_dir" ----> commented fails due to subprocess error....
        # # set +x  # Disable command tracing
    else
        echo "Skipping $rid - no PennEPI dataset assigned"
    fi
done