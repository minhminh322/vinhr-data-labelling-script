# Generate feedback

import pandas as pd 
import csv
import os
import shutil
from tqdm import tqdm
from pathlib import Path
from glob import glob
from datetime import datetime, timedelta

import sys
sys.path.append('..')

from src.utils import read_label_csv,\
                      to_Path,\
                      string_to_timedelta,\
                      strfdelta,\
                      strfdelta2,\
                      xou_calculation

# from config import config_1,\
#                     config_1_final,\
#                     config_2,\
#                     config_2_final

xou_percent = 0.2

def subtract_dateobject(dataframe_A, dataframe_C, filename_csv, path):
    """This function decides whether the marker is "OK" or need "To Check".

    Arguments:
        dataframe_A (dataframe) : first csv dataframe
        dataframe_C (dataframe) : merged csv dataframe

    Returns:
        feedback_csv (.csv) : output csv data file.
        feedback_path (path) : feedback files directory
    """
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    for index_1, rows_1 in dataframe_A.iterrows():
        # print(rows_1["Marker Name"])
        if(len(dataframe_C.index) != 0):
            
            for index_2, rows_2 in dataframe_C.iterrows():
                if rows_1["Marker Name"] == rows_2["Marker Name"]:
                    label_1_timestamp = (rows_1["Label_In"], rows_1["Label_Out"])
                    label_2_timestamp = (string_to_timedelta(rows_2["Label_In"]), 
                                         string_to_timedelta(rows_2["Label_Out"]))
                    
                    xou_result = xou_calculation(label_1_timestamp, label_2_timestamp)


                    if xou_result < xou_percent:
                        rows_1["Description"] = "OK"
                        
    dataframe_A.loc[dataframe_A["Description"]!='OK','Description'] = 'Bạn thiếu một label ở đây.'

    filename_csv = filename_csv.split("/")[-1]
    feedback_path = os.path.join(path,filename_csv)
    dataframe_A.to_csv(feedback_path, index=None)
    return feedback_path


def feedback_generator(feedback_path1, feedback_path2, merged_csv):
    """Generate feedback from two .csv files and a merged csv file.

    Arguments:
        filename_csv_1 (string) : first csv data file
        filename_csv_2 (string) : second csv data file
        merge_csv (string) : merged csv from 2 csv data file

    """
    try:
        df_1 = pd.read_csv(feedback_path1)
        df_2 = pd.read_csv(feedback_path2)
    except Exception as e:
        df_1 = read_label_csv(feedback_path1)
        df_2 = read_label_csv(feedback_path2)
    
    # Add feedback to team 1
    count_feedback_1, ok_1 = add_recommend_csv(feedback_path1, df_1, df_2)
    # Add feedback to team 2
    count_feedback_2, ok_2 = add_recommend_csv(feedback_path2, df_2, df_1)

    return count_feedback_1, ok_1, count_feedback_2, ok_2


def add_recommend_csv(feedback_path, df_1, df_2):
    """Generate recommend description for a given feedback file.

    Arguments:
        feedback_csv (string) : feedback csv data file
        df_1 (DataFrame) : first csv dataframe
        df_2 (DataFrame) : second csv dataframe

    Returns:
        feedback_with_recommend {.csv} -- export feedback csv file with recommends.
    """
    # Generate list colunm data of file csv merge 
    marker_names = []
    descriptions = []
    start_times = []
    end_times = []
    duration = []
    count  = 0
    ok = 0
    
    for index_1, rows_1 in df_1.iterrows():
        marker_names.append("")
        descriptions.append(rows_1['Description'])
        start_times.append(rows_1["Label_In"])
        end_times.append(rows_1["Label_Out"])
        duration.append(rows_1["Duration"])

    for index_2, rows_2 in df_2.iterrows():
        if (rows_2["Description"] == "Bạn thiếu một label ở đây."):
            marker_names.append("")
            descriptions.append('Bạn thiếu một label ở đây!')

            start_times.append(rows_2["Label_In"])
            end_times.append(rows_2["Label_Out"])

            duration.append(rows_2["Duration"])
            count += 1
        else:
            ok += 1

    # Genarate a merged csv file:
    file_csv_1 = {
        "Marker Name": marker_names,
        "Description": descriptions,
        "Label_In": start_times,
        "Label_Out": end_times,
        "Duration": duration,
        # "Recommend": recommend,
    }

    df_merger = pd.DataFrame(file_csv_1, columns = [
        "Marker Name",
        "Description",
        "Label_In",
        "Label_Out",
        "Duration",
        # "Recommend",
    ])
    # save csv file merge
    df_merger.to_csv(feedback_path, index = None)
    return count, ok

# def feedback_and_recommend(config.label_1_path, config.label_2_path, config.merge_path_fb, 
#                            config.mapping_path, config.feedback_1_path, config.feedback_2_path, config.dest_path, config.round_nums=1):

def feedback_and_recommend(config):
    # Use the default arugments
    """Generate feedback and recommend description for a given feedback file.

    Arguments:
        config.label_1_path (Path) : path label csv file team 1
        config.label_2_path (Path) : path label csv file team 2
        config.merge_path_fb (Path) : path merge csv file 
        config.mapping_path (Path) : path mapping file csv
        config.feedback_1_path (Path) : path save feedback file csv team 1
        config.feedback_2_path (Path) : path save feedback file csv team 2
        config.dest_path (Path) : first csv dataframe
        config.round_nums (Path) : second csv dataframe

    Returns:
        feedback_with_recommend (.csv) : export feedback csv file with recommends.
    """
    print(f'Output will be saved at: {config.dest_path_fb}')
    
    df = pd.read_csv(config.mapping_path)

    print("===ADD FEEDBACK===")
    for i, row in tqdm(df.iterrows()):
        try:
            if(config.round_nums == 1):
                path1 = str(config.label_1_path/f"{row.ID}.MOV.csv")
                path2 = str(config.label_2_path/f"{row.ID}.MOV.csv")
            else:
                path1 = str(config.label_1_path/row.Tagname/f"{row.ID}.MP4.csv")
                path2 = str(config.label_2_path/row.Tagname/f"{row.ID}.MP4.csv")

            # read data frame
            df_1 = read_label_csv(path1, True)
            df_2 = read_label_csv(path2, True)
            
            # read dataframe in file csv merge
            if(config.round_nums == 1):
                config.merge_path_fb_1 = os.path.join(config.merge_path_fb, f"{row.ID}.csv")
            else:
                filename_csv = row.ID.split('.')[0]
                config.merge_path_fb_1 = os.path.join(config.merge_path_fb/row.Tagname, f"{filename_csv}.csv")
                
            df_merge = pd.read_csv(config.merge_path_fb_1)

            # add feadback (ok, to check) to csv file label team
            if(config.round_nums == 1):
                subtract_dateobject(df_1, df_merge, f"{row.ID}.csv", config.feedback_1_path)
                subtract_dateobject(df_2, df_merge, f"{row.ID}.csv", config.feedback_2_path)
            else:
                filename_csv = row.ID.split('.')[0]
                subtract_dateobject(df_1, df_merge, f"{filename_csv}.csv", config.feedback_1_path/row.Tagname)
                subtract_dateobject(df_2, df_merge, f"{filename_csv}.csv", config.feedback_2_path/row.Tagname)

        except Exception as e :
            print(e)

    print("===ADD RECOMMEND===")
    for i, row in tqdm(df.iterrows()):
        try:
            if(config.round_nums == 1):
                path_feedback_1 = str(config.feedback_1_path/f"{row.ID}.csv")
                path_feedback_2 = str(config.feedback_2_path/f"{row.ID}.csv")

                config.merge_path_fb_2 = os.path.join(config.merge_path_fb, f"{row.ID}.csv")
            else:
                filename_csv = row.ID.split('.')[0]
                path_feedback_1 = str(config.feedback_1_path/row.Tagname/f"{filename_csv}.csv")
                path_feedback_2 = str(config.feedback_2_path/row.Tagname/f"{filename_csv}.csv")

                config.merge_path_fb_2 = os.path.join(config.merge_path_fb/row.Tagname, f"{filename_csv}.csv")
            
            # add feedback to recommend to team labeling
            count_feedback_1, ok_feedback_1, count_feedback_2, ok_feedback_2 = feedback_generator(path_feedback_1, path_feedback_2, config.merge_path_fb_2)

        except Exception as e :
            print(e)


if __name__ == "__main__":
    pass
