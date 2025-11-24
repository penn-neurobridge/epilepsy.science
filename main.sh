#!/bin/bash
# 
# Pennsieve Dataset Curation Pipeline
# This script demonstrates the complete workflow for curating epilepsy.science datasets
#

echo "Starting Pennsieve Dataset Curation Pipeline..."

# Step 1: Start the Pennsieve agent
echo "Step 1: Starting Pennsieve agent..."
pennsieve agent &
sleep 2

# Step 2: Verify agent is running with profile
echo "Step 2: Verifying Pennsieve credentials..."
pennsieve whoami

# # Step 3: Get all epilepsy.science datasets from Pennsieve (optional - for reference)
# echo "Step 3: Fetching available datasets..."
# uv run get_pennseive_datasets.py

# # Step 4: Map the specific datasets to local
# echo "Step 4: Mapping datasets to local directory..."
# uv run map_pennseive_datasets.py --base-data-dir /app/data --dataset-name "PennEPI00143"
# # Note: PennEPI00143 corresponds to sub-RID0054

# # Step 5: Pull specific files or directories from Pennsieve
# echo "Step 5: Pulling specific files from Pennsieve..."
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00143/archive"
# Additional pull examples:
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/participants.tsv"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/participants.json"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/derivatives"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-postimplant/ct"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-postsurgery/anat"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-preimplant"

# # Step 7: Check differences between local and remote
echo "Step 7: Checking for differences between local and remote..."
uv run diff_pennseive_datasets.py --dataset-name "PennEPI00143" --base-data-dir /app/data

# # Step 8: Push changes back to Pennsieve
# echo "Step 8: Pushing changes back to Pennsieve..."
# # Dry run first to see what would be uploaded
# uv run push_pennseive_datasets.py -n "PennEPI00143" --dry-run --base-data-dir /app/data

# # # Uncomment to actually upload:
# uv run push_pennseive_datasets.py -n "PennEPI00143" --base-data-dir /app/data

# # Step 9: Verify upload (optional)
# echo "Step 9: Verifying upload..."
# uv run diff_pennseive_datasets.py --dataset-name "PennEPI00143" --base-data-dir /app/data

# echo "Pipeline complete!"
