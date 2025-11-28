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

# Read the CSV file and loop through each PennEPI value
# Skip the header line (first line) and skip empty PennEPI values
tail -n +2 data/id_map.csv | head -n 1 | while IFS=',' read -r rid hup pennepi priority expect_recon; do
    # Skip if PennEPI column is empty or just says "PennEPI"
    if [[ -n "$pennepi" && "$pennepi" != "PennEPI" ]]; then
        echo "Processing dataset: $pennepi (from $rid)"
        uv run map_pennseive_datasets.py --base-data-dir /app/data --dataset-name "$pennepi"
        uv run sync_ieeg-recon.py --rid "$rid" --pennepi "$pennepi"
        uv run diff_pennseive_datasets.py --dataset-name "$pennepi" --base-data-dir /app/data
        uv run push_pennseive_datasets.py --dataset-name "$pennepi" --base-data-dir /app/data
    else
        echo "Skipping $rid - no PennEPI dataset assigned"
    fi
done

# # Pull specific files or directories from Pennsieve
# echo "Pulling specific files from Pennsieve..."
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00143/archive"
# Additional pull examples:
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/participants.tsv"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/participants.json"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/derivatives"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-postimplant/ct"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-postsurgery/anat"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-preimplant"
