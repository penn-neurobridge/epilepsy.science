#%%
import subprocess
import pandas as pd
import logging
import map_pennseive_datasets as map_pennseive
import sync_IEEGrecon
import sync_voxtoolCT
import sync_freesurfer
import diff_pennseive_datasets as diff_pennseive
import push_pennseive_datasets as push_pennseive

from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

data_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/develop/infrastructure/epilepsy_science_curate/data"
bids_dir="/Users/nishant/Dropbox/Sinha/Lab/Research/projects/discover/epilepsy/epi_t3_iEEG/data/BIDS"

#%%
subprocess.run(['pennsieve', 'agent'], check=True)
subprocess.run(['pennsieve', 'whoami'], check=True)

id_map = pd.read_csv(Path(data_dir) / "id_map.csv")

#%%
# make an empty dataframe to recoerd if the dataset was already processed
processed_df = pd.DataFrame(columns=['PennEPI', 'RID', 'ieeg_recon'])

for index,subject in id_map.iterrows():
    try:
        log.info(f"Processing dataset: {subject['PennEPI']}")
        map_pennseive.main(base_data_dir=data_dir, dataset_name=subject['PennEPI'],remove_existing=False)
        sync_IEEGrecon.main(rid=subject['RID'], pennepi=subject['PennEPI'], 
            base_data_dir=data_dir, bids_data_dir=bids_dir)
        sync_voxtoolCT.main(rid=subject['RID'], pennepi=subject['PennEPI'], 
            base_data_dir=data_dir, bids_data_dir=bids_dir)
        sync_freesurfer.main(rid=subject['RID'], pennepi=subject['PennEPI'], 
            base_data_dir=data_dir, bids_data_dir=bids_dir)
        diff_pennseive.main(dataset_name=subject['PennEPI'], base_data_dir=data_dir, output_csv=None)
        push_pennseive.main(dataset_name=subject['PennEPI'], base_data_dir=data_dir)
        map_pennseive.main(base_data_dir=data_dir, dataset_name=subject['PennEPI'],remove_existing=True)
        log.info(f"{subject['PennEPI']} pushed to Pennsieve")
        processed_df = pd.concat([processed_df, pd.DataFrame({'PennEPI': [subject['PennEPI']], 'RID': [subject['RID']], 
                'ieeg_recon': [True], 'voxtool_ct': [True], 'freesurfer': [True]})])
        processed_df.to_csv(Path(data_dir) / "processed_datasets.csv", index=False)
    except Exception as e:
        log.error(f"Error processing dataset: {subject['PennEPI']}")
        log.error(e)
        processed_df = pd.concat([processed_df, pd.DataFrame({'PennEPI': [subject['PennEPI']], 'RID': [subject['RID']], 
            'ieeg_recon': [False], 'voxtool_ct': [False], 'freesurfer': [False]})])
        processed_df.to_csv(Path(data_dir) / "processed_datasets.csv", index=False)
        continue
