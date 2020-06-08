import requests

def get_station_information(url, timeout = 20):
    '''
    Function to access the Station Endpoint of citibike api
    replaces station_id with _id in each station object, provides a GeoJSON representation of lon/lat
    
    :param url: url to the citibike station endpoint
    :param timeout: when does the call time out?
    :return: object with stations (array)
    '''
    response = requests.get(url, timeout=timeout)
    data = response.json()

    if not 'data' in data:
        raise ValueError('Incorrect Format of feed ' + url + ': Missing "data".')
        return
    if not 'stations' in data['data']:
        raise ValueError('Incorrect Format of feed ' + url + ': Missing "data.stations".')
        return

    stations = data['data']['stations']
    for station in stations:
        station['_id'] = station.pop('station_id')
        station['geometry'] = {'type': 'Point',
                               'coordinates': [ station.pop('lon'), station.pop('lat') ] }

    return data['data']['stations']

def get_station_status(url, last_updated=None):
    '''
    Function to return the station status

    :param url: url to station status endpoint
    :return: array of status for all stations if feed provides more current data than the last import, None otherwise
    '''
    try:
        response = requests.get(url, timeout=20)
        feed_data = response.json()
        
        if 'data' not in feed_data:
            raise ValueError('Station Status is not provided in the correct format. "data" is missing.')
        if 'stations' not in feed_data['data']:
            raise ValueError('Station Status is not provided in the correct format. "data" is missing.')
    
        feed_last_updated = feed_data['last_updated']
        feed_ttl = feed_data['ttl']

        if feed_last_updated > last_updated['last_updated']:
            return feed_data

        else:
            return None
        
    except ValueError as e:
        print(e)
        