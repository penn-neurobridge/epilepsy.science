import logging
from enrich_eps_datasets import get_enriched_eps_datasets

if __name__ == "__main__":
    eps_datasets = get_enriched_eps_datasets()
    print(eps_datasets)