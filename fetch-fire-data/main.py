import functions_framework

import pandas as pd
from google.cloud import storage


@functions_framework.cloud_event
def fetch_fire_data():
    # Open VIIRS data from NASA FIRMS
    url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_SouthEast_Asia_24h.csv"

    df = pd.read_csv(url)
    df.head()

    # Generate datetime column
    df["acq_datetime"] = pd.to_datetime(
        df["acq_date"] + " " + df["acq_time"].apply(lambda x: f"{x//100:02d}:{x%100:02d}:00"))

    # Filter data to Indonesian territory bounding box
    longitude, latitude = ((95.2930261576, 141.03385176),
                           (-10.3599874813, 5.47982086834))

    df = df[(df["longitude"] >= longitude[0])
            & (df["longitude"] <= longitude[1])
            & (df["latitude"] >= latitude[0])
            & (df["latitude"] <= latitude[1])].reset_index(drop=True)

    # Save data to JSON format
    df[["latitude", "longitude", "acq_datetime", "frp"]] \
        .rename(columns={"acq_datetime": "timestamp", "frp": "intensity"}) \
        .to_json("fire.json", orient="records")

    # Upload csv to cloud storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("nasa-sparc.appspot.com")

    blob = bucket.blob("fire.json")
    blob.upload_from_filename("fire.json")
