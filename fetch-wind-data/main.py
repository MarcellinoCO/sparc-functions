import functions_framework
from google.cloud import storage

import re
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


@functions_framework.cloud_event
def fetch_wind_data(_):
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
