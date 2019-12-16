from glob import glob
from pathlib import Path
import pandas as pd
import numpy as np
from functools import partial
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import os
import pandas as pd 
import csv
import os
import shutil
from glob import glob
from string import Template
from datetime import datetime, timedelta
from src.utils import string_to_timedelta

Path.ls = lambda x : [o.name for o in x.iterdir()]
Path.ls_p = lambda x : [str(o) for o in x.iterdir()]
Path.str = lambda x : str(x)

def read_label_csv(f):
    """Read label csv.

    Arguments:        
        f (Path) : path file csv data
        
    Returns:
        df (DataFrame): Data frame
    """
    df = pd.read_csv(f)
    df["Label_In"] = df["Label_In"].apply(convert_stringtime_to_dateobject2)
    df["Label_Out"] = df["Label_Out"].apply(convert_stringtime_to_dateobject2)
    return df


def convert_stringtime_to_dateobject2(stringtime):
    """Convert string time to data object.

    Arguments:        
        stringtime (string) : string time
        
    Returns:
        dt2td(str2dt(stringtime)) (dataobject) : data object time
    """
    try:
        str2dt = lambda x : datetime.strptime(x, '%H;%M;%S;%f')
        dt2td = lambda x : timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)
        return dt2td(str2dt(stringtime))
    except:
        str2dt = lambda x : datetime.strptime(x, '%H:%M:%S:%f')
        dt2td = lambda x : timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)
        return dt2td(str2dt(stringtime))

def get_label_id(filename):
    """Get label id.

    Arguments:        
        filename (string) : file name csv
        
    Returns:
        int(filename.split('/')[-1].split('_')[1]) (int) : id number label id
    """
    # '../../data/round_2/5_merge_label_final/HB/100_1_100_1.csv' => return 1
    return int(filename.split('/')[-1].split('.')[0].split('_')[-1])

def get_data_csv(file_id, df_map, data_root_path):
    """Helper method for gen data with label.

    Arguments:        
        file_id (Path) : path data root
        df_map (Path) : path merge label round 1
        data_root_path (float) : path merge label final
        
    Returns:
        
    """
    row = df_map[df_map['ID_parent']==file_id]
#     watch_id = row['Watch_ID'].iloc[0]
    file_name = row['Data_filename'].iloc[0]
    return pd.read_csv((data_root_path/file_name).str(), parse_dates = ['loggingTime(txt)'])

def save_data_csv(file_id, df_map, data_root_path, df):
    """Helper method for gen data with label.

    Arguments:        
        file_id (Path) : path data root
        df_map (Path) : path merge label round 1
        data_root_path (float) : path merge label final
        
    Returns:
        
    """
    row = df_map[df_map['ID_parent']==file_id]
#     watch_id = row['Watch_ID'].iloc[0]
    file_name = row['Data_filename'].iloc[0]
    df.to_csv((data_root_path/file_name).str(), index=False)

def save_df(df, path):
    """Helper method for gen data with label.

    Arguments:        
        data_root_path (Path) : path data root
        label_round1_path (Path) : path merge label round 1
        label_root_path (float) : path merge label final
        dest_path (Path) : dest path save data with label
        report_mapping_path (Path) : report mapping file
        
    Returns:
        
    """
    if(os.path.isfile(path)):
        previous_df = pd.read_csv(path)
        previous_df = previous_df.append(df)
        previous_df.to_csv(path, index=False)
    else:
        df.to_csv(path, index=False)

def check_not_found_file(filename):
    """Check not found file.

    Arguments:        
        filename (Path) : path filename
        
    Returns:
        
    """
    if not ((filename).is_file()) : 
        print(f'File not found {config.data_root_path/file_name}')
        
        return True 
    
    else:
        return False

def gen_label_data(config):
    """Helper method for gen data with label.

    Arguments:        
        data_root_path (Path) : path data root
        label_round1_path (Path) : path merge label round 1
        label_root_path (float) : path merge label final
        dest_path (Path) : dest path save data with label
        report_mapping_path (Path) : report mapping file
        
    Returns:
        
    """
    
    acts = config.label_root_path.ls()

    df_map = pd.read_csv(config.report_mapping_path, parse_dates=['Video_starttime'])

    # define list file id for check read get data csv
    list_file_name = []

    for i, row in df_map.iterrows():

        print(f'{i}/{len(df_map)}')
#         try:
        if True:
            file_id = row['ID_parent']
            data_filename = row['Data_filename']
            vid_starttime = row['Video_starttime']
            label_folder = row['Tagname']
            row = df_map[df_map['ID_parent']==file_id]
            file_name = row['Data_filename'].iloc[0]
            

            if not ((config.data_root_path/file_name).is_file()) : 
                # print(f'File not found {config.data_root_path/file_name}')
                continue

            # get data frame from watch csv file
            if file_name in list_file_name : 
                print("list file name ==========", list_file_name)
                df = get_data_csv(file_id, df_map, config.dest_path)
            else:
                df = get_data_csv(file_id, df_map, config.data_root_path)
                df['label'] = 'OT'
                


            # find all the label that has the same ID
            label_files = glob((config.label_root_path/f'{label_folder}/{file_id}_*csv').str())
            print(f"Video {row['Video_starttime']}, {len(label_files)}")
            
            for f in label_files:

                label_id = get_label_id(f) # the index of round 1 label
                df_label_round1 = read_label_csv(config.label_round1_path/f"{file_id}.csv") # to get the off set to starttime
                label1_starttime = df_label_round1.loc[label_id, 'Label_In'] # offset with the vid_starttime
                label =  df_label_round1.loc[label_id, 'Marker Name']

                df_label_round2 = read_label_csv(f)


                df_label_round2["In_timestamp"] = df_label_round2["Label_In"] + vid_starttime
                df_label_round2["Out_timestamp"] = df_label_round2["Label_Out"] + vid_starttime

                for i2, row_label in df_label_round2.iterrows():
                    df.loc[(df['loggingTime(txt)']<=row_label['Out_timestamp']) & (df['loggingTime(txt)']>=row_label['In_timestamp']),'label'] = row_label['Marker Name'][: len(row_label['Marker Name']) - 1]
                    print(f"{row_label['Marker Name']} - In {row_label['In_timestamp']}, Out {row_label['Out_timestamp']} - len {len(df)}")

            # save data csv
            save_data_csv(file_id, df_map, config.dest_path, df)

            # append file name to list fle name
            if file_name not in list_file_name:
                list_file_name.append(file_name)

            

