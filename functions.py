import pandas as pd
from datetime import datetime as dt
import hashlib
import numpy as np
import json
import warnings
warnings.filterwarnings("ignore") 

def sha256(x):
    """
    Returns the SHA256 hash of the input.
    Sha256 is 1 way hashing algorithm, so should always produce the same output for a given input.
    """
    return hashlib.sha256(x.encode('utf-8')).hexdigest()

def isSubArray(A, B):
    """
    checks if list A is inside list B
    :param A (array): shorter list
    :param B (array): longer list
    :return:
    """
    if len(A) > len(B):
        return False
    for i in A:
        if i not in B:
            return False
    return True

def GetHistoricalData(path:str):
    """reads and cleans data from historical devices
    Args:
        path (str): path to the file
    Returns:
        [DataFrame]: dataframe with historical data formatted similar to new test data
    """
    # raises an exception if path is not .csv
    if not path.endswith('.csv'):
        raise Exception('file must be a csv')
    df = pd.read_csv(path)
    # raise excption if key columns not found
    if not isSubArray(['date','time','device_id','current',	'active_power',	'reactive_power','ts_id'],df.columns):
        raise Exception('key columns not found')
    df['timestamp'] = df[['date','time']].astype(str).apply(lambda x: dt.strptime(x.date + x.time, '%d/%m/%y%H:%M:%S'), axis=1)
    df = df.set_index('device_id')
    df.drop(columns=['date','time'], inplace=True)
    return df

def GetLKPData(path:str):
    """reads and cleans data from LKP json file
    Args:
        path (str): path to the file
    Returns:
        [DataFrame]: dataframe with LKP data
    """
    # raises an exception if file is not json
    if not path.endswith('.json'):
        raise Exception('file must be a json')
    df = pd.read_json(path).transpose()
    df.index.rename('device_id', inplace=True)
    return df

def GetNewData(path:str):
    """reads and cleans data from new devices
    Args:
        path (str): path to the file
    Returns:
        [DataFrame]: dataframe with new data
    """
    # raises an exception if path is not .csv
    if not path.endswith('.csv'):
        raise Exception('file must be a csv')
    df = pd.read_csv(path)
    df.rename(columns={'socket_id':'device_id'}, inplace=True)
    df.set_index('device_id', inplace=True)
    return df

def createJson(df, OutputPath:str):
    """
    Creates a summary JSON specified and returns it 
    Args:
        df (Pandas Dataframe): Dataframe to summarise
    returns: results json
    """
    df_devices = df.iloc[ np.unique( df.index.values, return_index = True )[1]]
    df_devices = df_devices[['device_type']]
    key_list = [sha256(x) for x in df_devices.index]
    id_mapping_dict = dict(zip(key_list, list(df_devices.index)))
    body_json = {
        "file": 'test',
        "Unique_devices": df.device_type.unique().tolist(),
        "device_counts": df_devices.device_type.value_counts().to_dict(),
        "objectsCopied": id_mapping_dict,
    }
    body = json.dumps(body_json)
    return body

def WriteOutJson(body, OutputPath:str):
    """
    Writes out the summary JSON specified path
    Args:
        body (str): JSON to write
    """
    with open(OutputPath, 'w') as f:
        f.write(body)

def JitterDates(df):
    """Turns the date of a timestamp field in a dataframe to 0001-01-01
    I don't know what you mean by Jitter the date, asumming you mean turn its all to 0001-01-01?
    Args:
        df (Dataframe): Dataframe to jitter
    Returns:
        [DataFrame]: Jittered dataframe
    """
    print("""I don't know what you mean by Jitter the date, asumming you mean turn its all to sth meaningless?""")
    df['timestamp'] = df['timestamp'].apply(lambda x: x.replace(day=1, month=1, year=1))
    return df

def FindandCleanErrors(df):
    """flags duplicate rows and erroneous sensor values and corrects them
    Args:
        df (Dataframe)
    :returns: Cleaned dataframe
    """
    df.sort_values(by=['device_id','timestamp'], ascending = True, inplace=True)
    df2 = df[df.duplicated()]
    duplicate_devices = []
    duplicate_TS = []
    for idx, val in  enumerate(df2['timestamp']):
        duplicate_devices.append(df.index[idx])
        duplicate_TS.append(val)
    df.reset_index(inplace=True)
    for device, ts in zip(duplicate_devices, duplicate_TS):
        repeat = 0
        for idx, val in enumerate(df['timestamp']):
            #I only need to change 1 of the duplicated values, not both
            if val == ts:
                if repeat ==1:
                    avg_dist = (pd.Timestamp(df.at[idx+1,'timestamp']) - pd.Timestamp(df.at[idx-1, 'timestamp']))/2
                    df.at[idx,'timestamp'] = avg_dist + pd.Timestamp(df.at[idx-1,'timestamp'])
                    print(f'corrected {val} to {df.at[idx,"timestamp"]} for {df.at[idx, "device_id"]}' )
                repeat +=1    
    for idx, val in enumerate(df['active_power']):
        if abs(val) > 10000:
            df.at[idx, 'active_power'] = (df.at[idx-1, 'active_power'] + df.at[idx+1, 'active_power'])/2
            print(f'Changed active_power {val} to {df.at[idx, "active_power"]}')
    for idx, val in enumerate(df['reactive_power']):
        if abs(val) > 10000:
            df.at[idx, 'reactive_power'] = (df.at[idx-1, 'reactive_power'] + df.at[idx+1, 'reactive_power'])/2
            print(f'Changed reactive_power {val} to {df.at[idx, "reactive_power"]}')
    return df
    