import time
from datetime import datetime, timedelta
import os, json
import numpy as np

import requests
from pathlib import Path
 
from earthscope_sdk import EarthScopeClient

client = EarthScopeClient()

def get_token(token_path='./'):

    # refresh the token if it has expired
    client.ctx.auth_flow.refresh_if_necessary()

    token = client.ctx.auth_flow.access_token
    
    return token

def get_es_file(url, directory_to_save_file='./', token_path='./'):

  # get authorization Bearer token
  token = get_token()

  # request a file and provide the token in the Authorization header
  file_name = Path(url).name

  r = requests.get(url, headers={"authorization": f"Bearer {token}"})
  if r.status_code == requests.codes.ok:
    # save the file
    with open(Path(Path(directory_to_save_file) / file_name), 'wb') as f:
        for data in r:
            f.write(data)
  else:
    #problem occured
    print(f"failure: {r.status_code}, {r.reason}")

    # https://gage-data.earthscope.org/archive/gnss/rinex/obs/<year>/<day>/<station><day>0.<two digit year>d.Z

directory_path = "./rinex_data"

os.makedirs(directory_path, exist_ok=True)

def download_rinex(doy, year, station):
    two_digit_year=str(year)[2:] #converts integer to string and slices the last characters
    for doy in np.arange(1,10):
        #download
        url='https://gage-data.earthscope.org/archive/gnss/rinex/obs/%d/%03d/%s%03d0.%sd.Z' %(year,doy,station,doy,two_digit_year)
        print('downloading: ', url)
        get_es_file(url, 'rinex_data')


station = "p038"
doy = 1
year = 2024

download_rinex(doy, year, station)