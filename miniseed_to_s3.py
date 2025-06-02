import requests
import csv
import boto3
from botocore.exceptions import ClientError
from datetime import date

# csv format
# station,"start_date","end_date"
# XD.MTAN.*,1/1/0194,12/31/1995

URL = "http://service.iris.edu/fdsnws/dataselect/1/query?"

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

def upload_to_s3(station):

    # s3 setup
    # s3_client = boto3.client('s3')
    session = boto3.Session(profile_name="spara")
    s3_client = session.client('s3')
    s3 = session.resource('s3')
    bucket_name = "my-miniseed"
    region_name = "us-east-2"
    try:
        s3_client.head_bucket(Bucket = bucket_name)
    except ClientError as error:
      error_code = int(error.response['Error']['Code'])
      if  error_code == 404:
        s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region_name})

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
        if day > 366:
            year = end_year
        else:
            year = start_year

        # STATION.NETWORK.YEAR.DAYOFYEAR
        # 'miniseed/TA/2004/365/A04A.TA.2004.365#2'
        s3_path_prefix = "/".join([station["station"], str(year), str(day)])
        file = ".".join([station["station"], station["network"], str(year), str(day)])
        key = "/".join([s3_path_prefix,file])

        r = requests.get(URL, params=params, stream=True)
        if r.status_code == requests.codes.ok:
            # save the file
            bucket = s3.Bucket(bucket_name)
            bucket.upload_fileobj(r.raw, key)
            # s3.meta.client(r.raw, bucket, key, s3_path_prefix)
        else:
            #problem occured
            print(f"failure: {r.status_code}, {r.reason}")

station_list = parse_stations("./one_day.csv")
for station in station_list:
    upload_to_s3(station)
