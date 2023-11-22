import pandas as pd
import json
from datetime import datetime
import s3fs
import requests

def run_property_etl():

    API_KEY = 'ZQMHHWEEFK'
    postcode = "EH548EG"
    bedrooms = 2
    url = f'https://api.propertydata.co.uk/rents?key={API_KEY}&postcode={postcode}&bedrooms={bedrooms}'

    #PULL DATA FROM API
    def pulledData(url):
        r = requests.get(url)
        return r.json()

    propertyData = pulledData(url)

    propertyList = []
    for property in propertyData["data"]["long_let"]["raw_data"]:
        redefined_data = {
            "price_per_week" : property["price"],
            "number_of_rooms" : property["bedrooms"],
            "property_type" :property["type"]
        }

        propertyList.append(redefined_data)

    df = pd.DataFrame(propertyList)
    df.to_csv(f"s3://property-data-youtube-bucket/{postcode}_data.csv")

    print("ETL process finished!!")