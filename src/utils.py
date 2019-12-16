from datetime import datetime, timedelta
from tqdm import tqdm
import pandas as pd
import csv
from pathlib import Path
Path.ls = lambda x : [o.name for o in x.iterdir()]
Path.ls_p = lambda x : [str(o) for o in x.iterdir()]
Path.str = lambda x : str(x)


def string_to_timedelta(stringtime):
    """ Convert the string time to timedelta object 
    Args: 
        stringtime(str): The string time E.g : '10:30:45:56'
    Returns: 
        (timedelta) : timedelta object of the input string
    """

    dt2td = lambda x : timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)

    if ';' in stringtime:
        str2dt = lambda x : datetime.strptime(x, '%H;%M;%S;%f')
    elif ':' in stringtime:
        str2dt = lambda x : datetime.strptime(x, '%H:%M:%S:%f')
    else: raise 'Invalid format'

    return dt2td(str2dt(stringtime))

def _read_label_premiere(csv_path):
    """
    Read file csv from exported by Premiere.
    The exported label file from Premiere has a different encoder.

    Arguments:
        csv_path (string) : Path to the csv file
    Returns:
        df (pd.DataFrame) : output csv data file.
    """
    df = pd.DataFrame()

    rows = []
    with open(csv_path, newline='', encoding="utf16") as f:
        csv_reader = csv.reader(f, delimiter='\t')
        for i, row in enumerate(csv_reader):
            if i == 0 : rows_1 = row
            else: rows.append(row)

    df = pd.DataFrame(rows, columns=rows_1)

    return df

def read_label_csv(csv_path, premiere = False):
    """
    Read the label file with In Out columns converted to timedelta
    Args: 
        csv_path (str) : Path to the label file
        premiere (bool) : True if this is the file output from Premiere, False if it's from merge function
    Return : 
        df (pd.DataFrame) : Data frame of label file
    """
    if premiere:

        df = _read_label_premiere(csv_path)
        df['In'] = df['In'].apply(string_to_timedelta)
        df['Out'] = df['Out'].apply(string_to_timedelta)
        df['Duration'] = df['Out'] - df['In']
            
        # make the name make more sense
        df.rename({
            'In' : "Label_In",
            'Out' : "Label_Out",
        }, axis=1, inplace=True)
            
    else:
        df = pd.read_csv(csv_path)

    return df


def cut_data_by_time(df_data, in_timestamp, out_timestamp, time_col = 'loggingTime(txt)'):
    """ Cut the dataframe given the in and out of timestamp
    Args:
        df_data (pd.DataFrame) : The data source to cut from
        in_timestamp (datetime|pd.datetime) : the start the time to cut
        out_timestamp (datetime|pd.datetime) : the endt the time to cut
        time_col (str) : the name of time column in df_data

    Returns:
        (pd.DataFrame) : cut data frame
    """
    assert isinstance(in_timestamp, timedelta), 'in_timestamp must be timedelta object'
    assert isinstance(out_timestamp, timedelta), 'out_timestamp must be timedelta object'

    df = df_data[(df_data[time_col]<=out_timestamp) & 
                     (df_data[time_col]>=in_timestamp)]
    return df

def to_Path(path):
    """ Convert a path string to Path object
    Returns path if it's already a Path object"""
    if not isinstance(path, Path) : return Path(path)
    else: return path

def xou_calculation(label_1_timestamp, label_2_timestamp):
    """Calcalate using XoU formular to product the threadhold percentage.
        XoU is opposite to IoU (Intersection over Union).

    Arguments:
        start_time_label_1 (DataObject) : start time label 1
        end_time_label_1 (DataObject) : end time label 1
        start_time_label_2 (DataObject) : start time label 2
        end_time_label_2 (DataObject) : end time label 2

    Returns:
        result_label {Float} -- Result of XoU function.
    """

    # Intersection of label_1 and label_2
    start_offset = abs(label_1_timestamp[0] - label_2_timestamp[0])

    end_offset = abs(label_1_timestamp[1] - label_2_timestamp[1])

    union_duration = (max(label_1_timestamp[1], label_2_timestamp[1]) 
                    - min(label_1_timestamp[0], label_2_timestamp[0]))
    
    try:
        xou_result = (start_offset + end_offset) / union_duration
    
    except Exception as e:
        return 0

    return xou_result


# Helper function which convert dataobject to string
def strfdelta(tdelta, fmt):
    d = {"D": tdelta.days}
    hours, rem = divmod(tdelta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    d["H"] = '{:02d}'.format(hours)
    d["M"] = '{:02d}'.format(minutes)
    d["S"] = '{:02d}'.format(seconds)
    t = DeltaTemplate(fmt)
    return t.substitute(**d)

def strfdelta2(td):
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    strtd = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}:{int(td.microseconds/10000):02d}"
    return strtd