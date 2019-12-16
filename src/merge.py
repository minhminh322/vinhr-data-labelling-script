# Merge_files
# Generate report

import pandas as pd 
from tqdm import tqdm
from pathlib import Path
import numpy as np


from src.utils import (read_label_csv, 
                       to_Path, 
                       strfdelta2,
                       xou_calculation)


def merge_helper(label_1_path, label_2_path, xou_value_input=0.2): 
    """Helper method for merging two given csv files.

    Arguments:
        
        label_1_path (Path) : first csv data file
        label_2_path (Path) : second csv data file
        xou_value_input (float) : xou value # default value = 0.2
    Returns:
        df_merge (DataFrame) : Merged dataframe.
        np.mean(xou_sum) (Float) : mean of xou value
    """

    df_1 = read_label_csv(label_1_path, True)
    df_2 = read_label_csv(label_2_path, True)
    # Check XoU function and merge 2 csv files

    merge_dict = {'Marker Name' : [], 
                'Description' : [], 
                'Label_In' : [], 
                'Label_Out' : [], 
                'XoU value' : [],
               }
    
    xou_sum = [] # use for calculating the mean of xou value.
    xou_mean = 0
    for index_1, rows_1 in df_1.iterrows():
        for index_2, rows_2 in df_2.iterrows():
            if (rows_1 ["Marker Name"] == rows_2["Marker Name"]):
                label_1_timestamp = (rows_1["Label_In"], rows_1["Label_Out"])
                label_2_timestamp = (rows_2["Label_In"], rows_2["Label_Out"])
                xou_result = xou_calculation(label_1_timestamp, label_2_timestamp) # to tuple
                
                # if the calculated xou value is less than the given xou threshold, we will merge them.
                if (xou_result < xou_value_input):
                    xou_sum.append(xou_result)
                    merge_dict['Marker Name'].append(rows_1['Marker Name'])
                    merge_dict['Description'].append(rows_1['Description'])

                    start_time = max(label_1_timestamp[0], label_2_timestamp[0])
                    merge_dict['Label_In'].append(strfdelta2(start_time))

                    end_time = min(label_1_timestamp[1], label_2_timestamp[1])
                    merge_dict['Label_Out'].append(strfdelta2(end_time))

                    merge_dict['XoU value'].append(xou_result)
    
    df_merge = pd.DataFrame(merge_dict)
    if xou_sum == []: xou_mean = 0
    else: xou_mean = np.mean(xou_sum)

    return df_merge, xou_mean
              

# def merge(config.label_1_path, config.label_2_path, config.mapping_path, config.dest_path, config.xou_value_input=0.2, config.round_nums=1):
def merge(config, part_nums=1):
    
    
    """Merge 2 input csv files into a merged csv.

    Arguments:
        
        config.label_1_path (Path) : first csv data file
        config.label_2_path (Path) : second csv data file
        config.mapping_path (Path) : mapping csv file
        config.dest_path (Path) : merged csv file destination
        config.xou_value_input (float) : xou value
        config.round_nums (int) : 1 if merging round 1, 
                           2 if merging round 2
    
    Returns:
        None
    """
    if part_nums == 1: print(f'Output will be saved at: {config.dest_path}')
    else: print(f'Output will be saved at:{config.dest_final_path}')
   
    
    df_mapping = read_label_csv(config.mapping_path, False)
#     else: df_mapping = read_label_csv(config.mapping_path, False)

    for i, row in tqdm(df_mapping.iterrows()):  
        # create an unique column in the mapping file for id_video
        id_video = row.ID
        
        if config.round_nums == 1:
            if(part_nums == 1):
                team_1_csv_path = Path(f'{config.label_1_path}/{id_video}.MOV.csv')
                team_2_csv_path = Path(f'{config.label_2_path}/{id_video}.MOV.csv')
            else: 
                team_1_csv_path = Path(f'{config.label_1_final_path}/{id_video}.MOV.csv')
                team_2_csv_path = Path(f'{config.label_2_final_path}/{id_video}.MOV.csv')
        else:
            if (part_nums == 1):
                team_1_csv_path = Path(f'{config.label_1_path}/{row.Tagname}/{id_video}.MP4.csv')
                team_2_csv_path = Path(f'{config.label_2_path}/{row.Tagname}/{id_video}.MP4.csv')   
            else:
                team_1_csv_path = Path(f'{config.label_1_final_path}/{row.Tagname}/{id_video}.MP4.csv')
                team_2_csv_path = Path(f'{config.label_2_final_path}/{row.Tagname}/{id_video}.MP4.csv') 
        
        # ignore if either team 1 or team 2 csv files is not found. 
        if not (team_1_csv_path.is_file() and team_2_csv_path.is_file()) : 
            df_mapping.loc[i, 'XoU_mean'] = None
            df_mapping.loc[i, 'Number_of_merged_label'] = 'File not found'
#             print(f'File not found {team_1_csv_path if  not team_1_csv_path.is_file() else team_2_csv_path}')
            continue
        
        df_merge, xou_mean = merge_helper(team_1_csv_path, team_2_csv_path, config.xou_value_input)
        
        # Add XoU_mean and Number_of_merged_label columns to DataFrame
        df_mapping.loc[i, 'XoU_mean'] = xou_mean 
        df_mapping.loc[i, 'Number_of_merged_label'] = len(df_merge) 
        
        if config.round_nums == 1:
            if part_nums == 1:
                df_merge.to_csv(f'{config.dest_path}/{id_video}.csv', index=False)
                df_mapping.to_csv(config.mapping_path, index=False)
            else:
                df_merge.to_csv(f'{config.dest_final_path}/{id_video}.csv', index=False)
                df_mapping.to_csv(config.mapping_path, index=False)
                
        else:
            if part_nums == 1:
                id_video = id_video.split('.')[0]
                dest_path = f"{config.dest_path}/{row.Tagname}/"
                Path(dest_path).mkdir(parents=True, exist_ok = True)

                df_merge.to_csv(f'{dest_path}/{id_video}.csv', index=False)
                df_mapping.to_csv(config.mapping_path, index=False)
            else:
                id_video = id_video.split('.')[0]
                dest_final_path = f"{config.dest_final_path}/{row.Tagname}/"
                Path(dest_final_path).mkdir(parents=True, exist_ok = True)

                df_merge.to_csv(f'{dest_final_path}/{id_video}.csv', index=False)
                df_mapping.to_csv(config.mapping_path, index=False)
                
