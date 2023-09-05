import os

import requests
from bs4 import BeautifulSoup

from lib.models import HighDensityObservation, Mission, Observation, Storm, session

hdobs_dir = "./hdobs"


def insert_storm(hdob):
    new_storm = Storm(
        ocean_basin=hdob["ocean_basin"],
        name=hdob["storm_name"],
        number=hdob["storm_number"],
        year=hdob["date"][0:4]
    )
    session.add(new_storm)
    session.commit()


def insert_mission(storm_id, hdob):
    new_mission = Mission(
        callsign=hdob["callsign"],
        mission_number=hdob["mission_number"],
        storm_id=storm_id
    )
    session.add(new_mission)
    session.commit()


def insert_high_density_observation(mission_id, hdob):
    new_hdob_observation = HighDensityObservation(
        mission_id=mission_id,
        observation_number=hdob["observation_number"],
        product=hdob["product"],
        transmitted=hdob["transmitted"]
    )
    session.add(new_hdob_observation)
    session.commit()


def insert_observation(high_density_observation_id, hdob):
    for observation in hdob["observations"]:
        observation["high_density_observation_id"] = high_density_observation_id
    session.bulk_insert_mappings(Observation, hdob["observations"])
    session.commit()


def insert_hdobs():
    files = os.listdir(hdobs_dir)

    hdob = {}
    observations = []

    for file in files:
        with open(f"{hdobs_dir}/{file}", "r") as f:
            lines = f.readlines()

        for line_num, line in enumerate(lines):
            if line.startswith("$$") or line.startswith(";"):
                continue

            if line.strip():
                parts = line.split()

                if line_num == 2:
                    match parts[0]:
                        case "URNT15":
                            hdob["ocean_basin"] = "atlantic"
                        case "URPN15":
                            hdob["ocean_basin"] = "east and central pacific"
                        case "URPA15":
                            hdob["ocean-basin"] = "west pacific"

                    hdob["product"] = f"{parts[0]} {parts[1]}"
                    hdob["transmitted"] = parts[2]

                if line_num == 3:
                    parts_1 = list(parts[1])

                    hdob["callsign"] = parts[0]

                    # skip training missions
                    if "".join(parts_1) == "WXWXA":
                        hdob["mission_number"] = ""
                        hdob["storm_number"] = ""
                    else:
                        hdob["mission_number"] = "".join(parts_1[0:2])
                        hdob["storm_number"] = "".join(parts_1[2:4])
                    hdob["storm_name"] = parts[2].lower()
                    hdob["observation_number"] = parts[4]
                    hdob["date"] = parts[5]

                if line_num > 3:
                    parts_0 = list(parts[0])
                    parts_8 = list(parts[8])

                    observations_dict = {
                        "hour": "".join(parts_0[0:2]),
                        "minute": "".join(parts_0[2:4]),
                        "second": "".join(parts_0[4:]),
                        "coordinates": parts[1] + parts[2],
                        "aircraft_static_air_pressure": parts[3],
                        "aircraft_geopotential_height": None if str(parts[4]) == "////" else parts[4],
                        "extrapolated_surface_pressure": None if str(parts[5]) == "////" else parts[5],
                        "air_temperature": None if str(parts[6]) == "////" else parts[6],
                        "dew_point": None if str(parts[7]) == "////" else parts[7],
                        "wind_direction": None if str("".join(parts_8[0:3])) == "////" else "".join(parts_8[0:3]),
                        "wind_speed": None if str("".join(parts_8[0:3])) == "////" else "".join(parts_8[3:]),
                        "peak_wind_speed": None if str(parts[9]) == "///" else parts[9],
                        "sfmr_peak_surface_wind_speed": None if str(parts[10]) == "///" else parts[10],
                        "sfmr_surface_rate_rate": None if str(parts[11]) == "///" else parts[11],
                        "quality_control_flags": parts[12]
                    }

                    observations.append(observations_dict)

        hdob["observations"] = observations

        # skip training missions
        if hdob["mission_number"] != "":
            existing_storm = session.query(Storm).filter_by(name=hdob["storm_name"]).first()

            if existing_storm is None:
                insert_storm(hdob)

            storm_id = session.query(Storm).filter_by(name=hdob["storm_name"]).first().id

            existing_mission = session.query(Mission).filter_by(
                callsign=hdob["callsign"],
                mission_number=hdob["mission_number"],
                storm_id=storm_id
            ).first()

            if existing_mission is None:
                insert_mission(storm_id, hdob)

            mission_id = session.query(Mission).filter_by(
                callsign=hdob["callsign"],
                mission_number=hdob["mission_number"],
                storm_id=storm_id
            ).first().id

            existing_high_density_observation = session.query(HighDensityObservation).filter_by(
                mission_id=mission_id,
                observation_number=hdob["observation_number"],
                transmitted=hdob["transmitted"]
            ).first()

            if existing_high_density_observation is None:
                insert_high_density_observation(mission_id, hdob)

                high_density_observation_id = session.query(HighDensityObservation).filter_by(
                    mission_id=mission_id,
                    observation_number=hdob["observation_number"],
                    transmitted=hdob["transmitted"]
                ).first().id

                insert_observation(high_density_observation_id, hdob)

        observations.clear()
        hdob.clear()


def collect_hdobs(db_insert=False):
    url = "https://www.nhc.noaa.gov/archive/recon/2023/AHONT1"
    response = requests.get(url)

    hdobs = []

    if response.status_code == 200:
        html = response.text

        with open("recon.html", "w", encoding="utf-8") as f:
            f.write(html)

    with open("recon.html", "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), features="html.parser")

        for link in soup.find_all(name="a", href=True):
            hdob = str(link["href"])

            if hdob.startswith("AHONT1"):
                hdobs.append(hdob)

    if not os.path.exists(hdobs_dir):
        os.makedirs(hdobs_dir)

    for hdob in hdobs:
        if not os.path.exists(f"{hdobs_dir}/{hdob}"):
            response = requests.get(f"{url}/{hdob}")

            if response.status_code == 200:
                with open(f"{hdobs_dir}/{hdob}", "w") as f:
                    f.write(response.text)

    os.remove("recon.html")

    if db_insert is True:
        insert_hdobs()
