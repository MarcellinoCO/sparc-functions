import functions_framework
from google.cloud import storage

import pandas as pd
import numpy as np

import re
import json
import urllib.request
import subprocess


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
    df = df[df["confidence"] == "high"].reset_index(drop=True)

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
    with open("wind.grb", "wb") as file:
        file.write(file_content)

    # Run Grib2Json conversion
    command = [
        "converter/bin/grib2json",
        "--data",
        "--output", "wind.json",
        "--names",
        "--compact",
        "wind.grb"
    ]

    subprocess.run(command, capture_output=True, text=True)

    # Upload json to cloud storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("nasa-sparc.appspot.com")

    blob = bucket.blob("wind.json")
    blob.upload_from_filename("wind.json")


def compute_zones(yellow_zone_extension=0.5):
    # Load fire and wind data
    with open("fire.json", "r") as file:
        fire_data = json.load(file)

    with open("wind.json", "r") as file:
        wind_data = json.load(file)

    # Extracting U-component, V-component, latitude, and longitude grids
    u_component_data = wind_data[0]['data']
    v_component_data = wind_data[1]['data']

    nx = wind_data[0]['header']['nx']
    ny = wind_data[0]['header']['ny']

    latitude_grid = np.linspace(wind_data[0]['header']['la1'], wind_data[0]['header']['la2'], ny)
    longitude_grid = np.linspace(wind_data[0]['header']['lo1'], wind_data[0]['header']['lo2'], nx)

    u_component_grid = np.array(u_component_data).reshape(ny, nx)
    v_component_grid = np.array(v_component_data).reshape(ny, nx)
    
    # Extracting fire coordinates and fire intensities
    fire_coords = [(event['latitude'], event['longitude'], event['intensity']) for event in fire_data]
    max_frp = max([coord[2] for coord in fire_coords])
    
    # Function to find the nearest grid point for given coordinates
    def nearest_grid_point(lat, lon, grid_lats, grid_lons):
        lat_idx = np.argmin(np.abs(grid_lats - lat))
        lon_idx = np.argmin(np.abs(grid_lons - lon))

        return lat_idx, lon_idx
    
    # Extract wind data for each fire coordinate
    fire_wind_data = []
    for coord in fire_coords:
        lat, lon, frp = coord
        normalized_frp = frp / max_frp

        lat_idx, lon_idx = nearest_grid_point(lat, lon, latitude_grid, longitude_grid)

        u_wind = u_component_grid[lat_idx, lon_idx] * (1 + normalized_frp)
        v_wind = v_component_grid[lat_idx, lon_idx] * (1 + normalized_frp)

        fire_wind_data.append({
            "lat": lat,
            "lon": lon,
            "u_wind": u_wind,
            "v_wind": v_wind
        })
    
    # Compute new coordinates and areas based on wind data
    def compute_dispersed_coords_and_areas(lat, lon, u_wind, v_wind, scaling_factor=1.0):
        R = 6371.0  # Earth's radius in km

        dispersal_distance = np.sqrt(u_wind**2 + v_wind**2) * scaling_factor
        red_zone_area = np.pi * dispersal_distance**2

        yellow_zone_distance = dispersal_distance * (1 + yellow_zone_extension)
        yellow_zone_area = np.pi * (yellow_zone_distance**2 - dispersal_distance**2)
        
        delta_lat = (v_wind * scaling_factor) / R * (180.0 / np.pi)
        delta_lon = (u_wind * scaling_factor) / (R * np.cos(np.pi * lat / 180.0)) * (180.0 / np.pi)

        new_lat = lat + delta_lat
        new_lon = lon + delta_lon

        return new_lat, new_lon, red_zone_area, yellow_zone_area

    # Compute dispersed coordinates and areas for each fire location
    dispersed_data_with_areas = []
    for entry in fire_wind_data:
        lat, lon, u_wind, v_wind = entry['lat'], entry['lon'], entry['u_wind'], entry['v_wind']

        red_lat, red_lon, red_area, yellow_area = compute_dispersed_coords_and_areas(lat, lon, u_wind, v_wind)
        yellow_lat, yellow_lon = compute_dispersed_coords_and_areas(lat, lon, u_wind * (1 + yellow_zone_extension), v_wind * (1 + yellow_zone_extension))[:2]
        
        dispersed_data_with_areas.append({
            "lat": lat,
            "lon": lon,
            "red_lat": red_lat,
            "red_lon": red_lon,
            "red_area": red_area,
            "yellow_lat": yellow_lat,
            "yellow_lon": yellow_lon,
            "yellow_area": yellow_area
        })

    return dispersed_data_with_areas


@functions_framework.cloud_event
def simulate_smoke(_):
    fetch_fire_data()
    fetch_wind_data()
    smokes = compute_zones()

    with open("smoke.json", "w") as file:
        json.dump(smokes, file)

    # Upload json to cloud storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("nasa-sparc.appspot.com")

    blob = bucket.blob("smoke.json")
    blob.upload_from_filename("smoke.json")
