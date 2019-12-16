from tqdm import tqdm
import pandas as pd
from pathlib import Path
from glob import glob
from src.utils import *

def mapping_csv(round_1_mapping_csv_path, label_src):
    """
    Concatenate all the labels files and map with its orignal information from mapping file 
    Args : 
        pre_mapping_csv_path (Path) : Path to the mapping file
        label_src (Path) : Path to the src of all labels files
    return : 
        df_all_labels_merged (pd.DataFrame) : Data frame contains all labels and related information

    """
    df_map = pd.read_csv(round_1_mapping_csv_path, parse_dates=['Video_starttime'])
    label_src = to_Path(label_src)

    df_all_labels = pd.DataFrame()
    # find all the label of round 1
    all_label_csvs = glob((label_src/'*csv').str())
    assert len(all_label_csvs) > 0, "Label files not found"
    for csv in all_label_csvs:
        df = read_label_csv(csv)
        
        
        df['Label_In'] = df['Label_In'].apply(string_to_timedelta)
        df['Label_Out'] = df['Label_Out'].apply(string_to_timedelta)
        df['Duration'] = df['Label_Out'] - df['Label_In']
        # make the name make more sense
#         df.rename({
#                 'In' : "Label_In",
#                 'Out' : "Label_Out",
#             }, axis=1, inplace=True)
#         videoID = csv.split('/')[-1]
#         print(csv)
        videoID = int(csv.split('/')[-1].split('.')[0])
        df['ID'] = int(videoID) # assign
        
        # assign the name of video after cut with each labels
        df['ID_child'] = df['ID'].astype(str) + '_' + df.index.astype(str)
        
        df_all_labels = df_all_labels.append(df)

    # mapping the info
    df_all_labels_merged = df_all_labels.merge(df_map, on='ID')
    
    # for better name meaning
    df_all_labels_merged.rename({'ID' : "ID_parent", 'ID_child' : 'ID'}, axis=1, inplace=True)
    

    # reassign new Video_starttime
    df_all_labels_merged['Video_endtime'] = df_all_labels_merged['Video_starttime'] + df_all_labels_merged['Label_Out']
    df_all_labels_merged['Video_starttime'] = df_all_labels_merged['Video_starttime'] + df_all_labels_merged['Label_In']
    df_all_labels_merged['Video_duration'] = df_all_labels_merged['Video_endtime'] - df_all_labels_merged['Video_starttime'] 
    
    df_all_labels_merged.rename({"Marker Name" : "Tagname"}, axis=1, inplace=True)

    return df_all_labels_merged 
    