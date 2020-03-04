# ipfx branch abf_nwb in ben-dichter-consulting GitHub
# requires ndx-labmetadata-abf

from glob import glob
import os
from tqdm import tqdm
from ipfx.x_to_nwb.ABFConverter import ABFConverter
import pandas as pd
from datetime import datetime
import numpy as np


def get_metadata(abf_num, df):
    if abf_num not in df['abf file'].values:
        return
    row = df[df['abf file'] == abf_num]
    cell_num = row['cell number'].values[0]
    if np.isnan(cell_num):
        cell_id = None
    else:
        cell_id='c' + str(int(cell_num))
    
    slice_num = row['slice'].values[0]
    if np.isnan(slice_num):
        tissue_sample_id = None
    else:
        tissue_sample_id = str(row['subject'].values[0]) + '-' + str(int(slice_num))
            
    meta = {
        'NWBFile': {
            'session_start_time': datetime.strptime(str(row['date'].values[0]),'%Y%m%d')
        },
        'Subject': {
            'species': 'homo sapiens',
            'subject_id': str(row['subject'].values[0]),
            'age': "Gestational Week {}".format(row['age (gestation week)'].values[0])
        },
        'lab_meta_data': {
            'cell_id': cell_id,
            'tissue_sample_id': tissue_sample_id
        }
    }
    return meta

df = pd.read_csv('/Volumes/easystore5T/data/DANDI/nwb_lizhou/recieved/lizhou_meta_denormalized.csv')

src_dir = '/Volumes/easystore5T/data/DANDI/nwb_lizhou/recieved/*/*.abf'
src_paths = glob(src_dir)

overwrite = False

for src_path in tqdm(list(src_paths)):
    #try:
        src_fname = os.path.split(src_path)[1]
        dest_fname = src_fname[:-3] + 'nwb'
        dest_path = '/'.join(src_path.split('/')[:6] + ['nwb', dest_fname])
        abf_num = int(src_fname[:-4])
        metadata = get_metadata(abf_num, df)
        
        if os.path.isfile(dest_path) and not overwrite:
            print('skipping {}.'.format(src_fname))
        else:
            print('converting {}.'.format(src_fname))
            kwargs = dict()
            if metadata is None:
                print('no metadata found for {}.'.format(src_fname))
            else:
                kwargs.update(metadata=metadata)
            
            ABFConverter(src_path, dest_path, outputFeedbackChannel=True, **kwargs)
