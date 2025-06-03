import requests
import csv, os
from datetime import date
from pathlib import Path

# SAGE archive
URL = "http://service.iris.edu/fdsnws/dataselect/1/query?"

# csv format
# station,"start_date","end_date"
# XD.MTAN.*,1/1/1994,12/31/1995
def parse_row(row):
    nsl = row[0].split('.')
    net = nsl[0]
    sta = nsl[1]
    if nsl[2] == '':
        loc = '*'
    else:
        loc = nsl[2]
    st = row[1]
    et = row[2]

    station = {"network": net, 
               "station": sta, 
               "location": loc, 
               "channel": "*", 
               "start_time": st, 
               "end_time": et}
    return station

def parse_stations(file):
    stations = []
    try:
        with open(file, 'r') as file:
            heading = next(file)
            csv_reader = csv.reader(file)
            for row in csv_reader:
                station = parse_row(row)
                stations.append(station)
    except FileNotFoundError:
        print(f"Error: File not found at path: {file}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return stations

def download(station):
    # duration
    start_year = int(station["start_time"][:4])
    end_year = int(station["end_time"][:4])
    params = {"net" : station["network"],
              "sta" : station["station"],
              "loc" : station["location"],
              "cha" : station["channel"],
              "start": station["start_time"] + "T00:00:00", 
              "end": station["end_time"] + "T00:00:00"}
    days = date.fromisoformat(station["end_time"]) - date.fromisoformat(station["start_time"])
    total_days = days.days
    
    # upload miniseed to s3
    for day in range(1,total_days + 1):
        # this only works for 2 years
        if day > 366:
            year = end_year
        else:
            year = start_year

        # file name format: STATION.NETWORK.YEAR.DAYOFYEAR
        # directory_path: './data'
        directory_path = "./data"
        file_name = ".".join([station["station"], station["network"], str(year), str(day)])

        r = requests.get(URL, params=params, stream=True)
        if r.status_code == requests.codes.ok:
            # save the file
            with open(Path(Path(directory_path) / file_name), 'wb') as f:
                for data in r:
                    f.write(data)
        else:
            #problem occured
            print(f"failure: {r.status_code}, {r.reason}")


# create directory for data
directory_path = "./miniseed_data"
os.makedirs(directory_path, exist_ok=True)

station_list = parse_stations("./one_day.csv")
for station in station_list:
    download(station)
