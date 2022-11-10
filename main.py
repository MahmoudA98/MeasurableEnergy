from functions import *


if __name__ == '__main__':
    lkp = GetLKPData(r'data\device.json')
    Historical_df = GetHistoricalData(r'data\historical.csv')
    new_df = GetNewData(r'data\test_7.csv')
    df = pd.concat([Historical_df, new_df], axis=0)
    # join and filter out unneeded columns
    df = pd.merge(left=df, right=lkp,how='inner',left_index=True, right_index=True)
    df = df[['timestamp', 'current', 'active_power', 'reactive_power','ts_id','device_type','device_location','device_brand','device_category', 'description', 'synthetic']]
    body = createJson(df, 'results.csv')
    WriteOutJson(body, 'dataset.json')
    df = FindandCleanErrors(df)
    # anonymising device_id
    df['device_id'] = df['device_id'].apply(lambda x: sha256(x))
    print(""" I didn't understand what you meant by jitter the date, but I wrote a function called JitterDates, which i think does what you want?""")
    print(""" I also don't fully understand the meaning of the sampling column, anyways I have spent about 2.5 hours on this, so will stop now""")
    df.to_csv('results.csv', index=False)