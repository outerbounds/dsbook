from datetime import datetime, timedelta

HISTORY_API = 'https://api.openweathermap.org/data/3.0/onecall'

def get_historical_weather_data(appid, lat, lon):
    import pandas as pd
    import requests

    now = datetime.utcnow() 
    data = []
    index = []
    for ago in range(5, 0, -1):
        tstamp = int((now - timedelta(days=ago)).timestamp())
        params = {'lat': lat, 'lon': lon, 'dt': tstamp,
                  'appid': appid, 'units': 'imperial'}
        reply = requests.get(HISTORY_API, params=params).json()
        for hour in reply['hourly']:  
            data.append(hour['temp'])
            index.append(datetime.utcfromtimestamp(hour['dt']))
    return pd.Series(data=data,
                     index=pd.DatetimeIndex(index, freq='infer'))

def series_to_list(series):
    index = map(lambda x: x.isoformat(), series.index)
    return list(zip(index, series))
