#%% 
from edfio import read_edf
from pathlib import Path
import matplotlib.pyplot as plt
import logging
from IPython import embed
logging.basicConfig(level=logging.INFO)

#%%
project_root = Path(__file__).parent
edf_file = Path(project_root / "data" / "local").rglob("*.edf")
edf_file = next(edf_file)
logging.info(f"Reading EDF file: {edf_file}")

#%%
edf_data = read_edf(edf_file)

#%%
fpz = edf_data.get_signal('Fpz')

data = fpz.get_data_slice(start_second=2018, stop_second= 2050)

#%%
plt.plot(data) 
plt.show()
