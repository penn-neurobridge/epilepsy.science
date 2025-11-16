#!/bin/bash

# Start the Pennsieve agent
pennsieve agent

# Test pennsieve agent is running with profile
pennsieve whoami

# Get all epilesy.science datasets from Pennsieve
# uv run get_pennseive_datasets.py

# Map the specific datasets to local
uv run map_pennseive_datasets.py --base-data-dir /app/data --dataset-name "PennEPI00143,PennEPI00049" 

# Pull the specific files or directories
uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00143/archive"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/participants.tsv"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/participants.json"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/derivatives"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-postimplant/ct"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-postsurgery/anat"
# uv run pull_pennseive_datasets.py -i "/app/data/output/PennEPI00049/primary/sub-PennEPI00049/ses-preimplant"


# Diff specific datasets
uv run diff_pennseive_datasets.py --dataset-name "PennEPI00143" --base-data-dir /app/data