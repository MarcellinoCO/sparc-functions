import functions_framework
from google.cloud import storage

import pandas as pd
from scipy.spatial import KDTree

import re
import json
import urllib.request
import subprocess
import math


def fetch_content(url):
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request) as response:
        content = response.read().decode("utf-8")

    return content


def fetch_file(url):
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request) as response:
        file_content = response.read()

    return file_content


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


def fetch_wind_data():
    # Fetch latest date
    gfs_date_content = fetch_content(
        "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl")

    date_pattern = r"https://nomads\.ncep\.noaa\.gov/cgi-bin/filter_gfs_0p25\.pl\?dir=%2F(gfs\.\d{8})"
    date_matches = re.findall(date_pattern, gfs_date_content)

    newest_date_subdir = date_matches[0]
    newest_date_link = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2F{newest_date_subdir}"

    # Fetch latest time (hour)
    gfs_time_content = fetch_content(newest_date_link)

    time_pattern = r"https://nomads\.ncep\.noaa\.gov/cgi-bin/filter_gfs_0p25\.pl\?dir=%2Fgfs\.\d{8}%2F(\d{2})"
    time_matches = re.findall(time_pattern, gfs_time_content)

    time_index = 0
    newest_time = time_matches[time_index]
    newest_time_link = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.20231008%2F{newest_time}%2Fatmos"

    # Check whether data is already available
    while True:
        gfs_files_content = fetch_content(newest_time_link)

        if "No files or directories found" not in gfs_files_content:
            break

        # If not available, fetch the next available time
        time_index += 1
        newest_time = time_matches[time_index]
        newest_time_link = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.20231008%2F{newest_time}%2Fatmos"

    # Filter the layers and bounding box
    longitude, latitude = ((95.2930261576, 141.03385176),
                           (-10.3599874813, 5.47982086834))

    filename = f"gfs.t{time_matches[time_index]}z.pgrb2.0p25.f000"
    nomads_url = newest_time_link + \
        f"&file={filename}&var_UGRD=on&var_VGRD=on&lev_10_m_above_ground=on&subregion=&leftlon={longitude[0]}&rightlon={longitude[1]}&toplat={latitude[1]}&bottomlat={latitude[0]}"

    # Download GFS file
    file_content = fetch_file(nomads_url)
    with open(filename, "wb") as file:
        file.write(file_content)

    # Run Grib2Json conversion
    command = [
        "converter/bin/grib2json",
        "--data",
        "--output", "wind.json",
        "--names",
        "--compact",
        filename
    ]

    subprocess.run(command, capture_output=True, text=True)

    # Upload json to cloud storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("nasa-sparc.appspot.com")

    blob = bucket.blob("wind.json")
    blob.upload_from_filename("wind.json")


@functions_framework.cloud_event
def simulate_smoke(_):
    fetch_fire_data()
    fetch_wind_data()

    with open("fire.json", "r") as fire_file:
        fire_data = json.load(fire_file)

    with open("wind.json", "r") as wind_file:
        wind_data = json.load(wind_file)

    def generate_lat_lon_pairs(header):
        latitudes, longitudes = [], []
        lo1, la1 = header['lo1'], header['la1']
        dx, dy = header['dx'], header['dy']
        nx, ny = header['nx'], header['ny']
        
        for i in range(ny):
            for j in range(nx):
                longitudes.append(lo1 + j * dx)
                latitudes.append(la1 + i * dy)
                
        return latitudes, longitudes
    
    flat_wind_coords, flat_wind_directions = [], []
    for entry in wind_data:
        latitudes, longitudes = generate_lat_lon_pairs(entry['header'])
        flat_wind_coords.extend(zip(latitudes, longitudes))
        flat_wind_directions.extend(entry['data'])

    wind_tree = KDTree(flat_wind_coords)

    def get_nearest_wind_direction(lat, lon):
        _, idx = wind_tree.query([lat, lon])
        return flat_wind_directions[idx]
    
    def generate_smoke_polygon(lat, lon, wind_direction, distance=1.0, width=0.01):
        angle_rad = math.radians(wind_direction)
        d_lat = distance * math.cos(angle_rad)
        d_lon = distance * math.sin(angle_rad)
        
        # Calculate perpendicular direction for width
        perp_angle_rad = angle_rad + math.pi / 2
        w_lat = width * math.cos(perp_angle_rad)
        w_lon = width * math.sin(perp_angle_rad)
        
        # Create polygon vertices in clockwise order
        p1 = (lat + w_lat, lon + w_lon)
        p2 = (lat - w_lat, lon - w_lon)
        p3 = (lat + d_lat - w_lat, lon + d_lon - w_lon)
        p4 = (lat + d_lat + w_lat, lon + d_lon + w_lon)
        
        return [p1, p2, p3, p4, p1]
    
    smoke_polygons = []
    for fire_point in fire_data:
        lat, lon = fire_point['latitude'], fire_point['longitude']
        wind_dir = get_nearest_wind_direction(lat, lon)
        smoke_polygon = generate_smoke_polygon(lat, lon, wind_dir)
        smoke_polygons.append(smoke_polygon)

    with open("smoke.json", "w") as file:
        json.dump(smoke_polygons, file)

    # Upload json to cloud storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("nasa-sparc.appspot.com")

    blob = bucket.blob("smoke.json")
    blob.upload_from_filename("smoke.json")