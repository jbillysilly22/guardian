#were def gonna need some libs up here soon!
import requests
import datetime


#national crime vitcim survey data fetcher kinda scared to use cuz its national but whatevs
def fetch_ncvm_data():
    ncvm_path = ('https://api.ojp.gov/bjsdataset/v1/')
    response = requests.get(ncvm_path)
    for i in response(1,20000):
        try:
             response = requests.get(ncvm_path)
             limiter.try_acquire("default")
        print(f"Request {i+1} allowed.")
    elif limiter.is_rate_limited("default")
       

    return response.json()




