import requests
import re
from datetime import datetime, timedelta

STATUS_API_ENDPOINT = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
STATION_API_ENDPOINT = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"


def get_status_info(timeout=20):
    response = requests.get(STATUS_API_ENDPOINT, timeout=timeout)
    data = response.json()
    stations = {}
    for station in data['data']['stations']:
        station['last_updated'] = data['last_updated']
        station['action'] = 'refreshStatus'
        stations[station['station_id']] = station
    return stations


def get_station_info(timeout=20):
    response = requests.get(STATION_API_ENDPOINT, timeout=timeout)
    data = response.json()
    stations = {}
    for station in data['data']['stations']:
        station['action'] = 'refreshStation'
        stations[station['station_id']] = station
    return stations

def get_full_station_info(timeout=20):
    stations = get_station_info(timeout=timeout)
    status = get_status_info(timeout=timeout)
    for key, station in stations.items():
        st = status[key]
        st.pop('status', None)
        station['status'] = st
        station['station_name'] = "{0}_{1}".format(station.get("station_id", ''), re.sub('[^A-Za-z0-9]+', '', station.get('name', '')))
        station['action'] = 'fullRefresh'
    return stations