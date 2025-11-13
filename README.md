# Neuronova Data Validator

This containerized microservice of the Neuronova application validates access and pulls epilepsy.science datasets from Pennsieve. It ensures all necessary neuroimaging and electrophysiology files are present to build a scalable normative IEEG atlas for applications in drug-resistant epilepsy.

## Prerequisites

- Docker (recommended)
- Make sure Docker is running before installation

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/penn-cnt/neuronova_data_validator.git
   cd neuronova_data_validator
   ```

2. **Run with Docker (Recommended):**
   ```bash
   make run
   ```

## Alternative Installation (Without Docker)

⚠️ **Note:** Docker is highly recommended for consistent environment setup.

If you choose to run without Docker:

1. **Install and start Pennsieve agent:**
   - Make sure the Pennsieve agent is installed and running

2. **Install UV package manager:**
   ```bash
   # Install uv if not already installed
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies:**
   ```bash
   uv sync
   ```

4. **Run the scripts:**
   ```bash
   uv run <script_name>.py
   ```

## Project Structure

- `main.sh` - Main execution pipeline
- `get_pennseive_datasets.py` - Fetches available datasets from Pennsieve
- `map_pennseive_datasets.py` - Maps datasets to local directory structure
- `pull_pennseive_datasets.py` - Downloads specific files from mapped datasets
- `validate_pennsieve_datasets.py` - Validates dataset completeness for downstream services
- `data/output/` - Output directory for processed data and validation results

## Usage

The service runs through a complete pipeline:
1. Connects to Pennsieve and authenticates
2. Retrieves available epilepsy.science datasets
3. Maps specific datasets (e.g., EPS0000049) to local structure
4. Pulls required files for analysis
5. Validates data completeness for downstream microservices

Check `data/output/` for results after running.
