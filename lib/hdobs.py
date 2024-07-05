import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from lib.models import HighDensityObservation, Mission, Observation, Storm, session

HDOBS_DIR = "./hdobs"


def quality_control_decoder(first_flag, second_flag):
    match first_flag:
        case "0":
            first_flag_decoded = "All parameters of nominal accuracy"
        case "1":
            first_flag_decoded = "Lat/lon questionable"
        case "2":
            first_flag_decoded = "Geopotential altitude or static pressure questionable"
        case "3":
            first_flag_decoded = "Both lat/lon and GA/PS questionable"
        case _:
            first_flag_decoded = ""

    match second_flag:
        case "0":
            second_flag_decoded = "All parameters of nominal accuracy"
        case "1":
            second_flag_decoded = "T or TD questionable"
        case "2":
            second_flag_decoded = "Flight-level winds questionable"
        case "3":
            second_flag_decoded = "SFMR parameter(s) questionable"
        case "4":
            second_flag_decoded = "T/TD and FL winds questionable"
        case "5":
            second_flag_decoded = "T/TD and SFMR questionable"
        case "6":
            second_flag_decoded = "FL winds and SFMR questionable"
        case "9":
            second_flag_decoded = "T/TD, FL winds, and SFMR questionable"
        case _:
            second_flag_decoded = ""

    return first_flag_decoded, second_flag_decoded


def insert_storm(hdob):
    new_storm = Storm(
        ocean_basin=hdob["ocean_basin"],
        name=hdob["storm_name"],
        number=hdob["storm_number"],
        year=hdob["date"][0:4],
    )
    session.add(new_storm)
    session.commit()


def insert_mission(storm_id, hdob):
    new_mission = Mission(
        callsign=hdob["callsign"], mission_number=hdob["mission_number"], storm_id=storm_id
    )
    session.add(new_mission)
    session.commit()


def insert_high_density_observation(mission_id, hdob):
    new_hdob_observation = HighDensityObservation(
        date=hdob["date"],
        file=hdob["file"],
        mission_id=mission_id,
        observation_number=hdob["observation_number"],
        product=hdob["product"],
        transmitted=hdob["transmitted"],
    )
    session.add(new_hdob_observation)
    session.commit()


def insert_observation(high_density_observation_id, hdob):
    for observation in hdob["observations"]:
        observation["high_density_observation_id"] = high_density_observation_id
    session.bulk_insert_mappings(Observation, hdob["observations"])
    session.commit()


def insert_hdobs():
    files = os.listdir(HDOBS_DIR)

    hdob = {}
    observations = []

    for file in files:
        hdob["file"] = file

        with open(f"{HDOBS_DIR}/{file}", "r") as f:
            lines = f.readlines()

        for line_num, line in enumerate(lines):
            if line.startswith("$$") or line.startswith(";"):
                continue

            if line.strip():
                parts = line.split()

                if line_num == 2:
                    match parts[0]:
                        case "URNT15":
                            hdob["ocean_basin"] = "Atlantic"
                        case "URPN15":
                            hdob["ocean_basin"] = "East and Central Pacific"
                        case "URPA15":
                            hdob["ocean_basin"] = "West Pacific"
                        case _:
                            hdob["ocean_basin"] = ""

                    hdob["product"] = f"{parts[0]} {parts[1]}"
                    hdob["transmitted"] = parts[2]

                if line_num == 3:
                    # mission numbers
                    parts_1 = list(parts[1])

                    hdob["callsign"] = parts[0]

                    training_missions = ["wxwxa", "wxwxe", "train"]

                    # handle training missions separately
                    if "".join(parts_1).lower() in training_missions:
                        hdob["mission_number"] = ""
                        hdob["storm_number"] = ""
                        continue
                    else:
                        hdob["mission_number"] = "".join(parts_1[0:2])
                        hdob["storm_number"] = "".join(parts_1[2:4])

                    hdob["storm_name"] = parts[2].title()
                    hdob["observation_number"] = parts[4]
                    hdob["date"] = parts[5]

                if line_num > 3:
                    # handle data format issues
                    try:
                        parts_0 = list(parts[0])
                        parts_3 = list(parts[3])
                        parts_5 = list(parts[5])
                        parts_6 = list(parts[6])
                        parts_7 = list(parts[7])
                        parts_8 = list(parts[8])
                        parts_12 = list(parts[12])
                    except IndexError:
                        continue

                    if str(parts[3]) == "////":
                        aircraft_static_air_pressure = None
                        aircraft_static_air_pressure_inhg = None
                    else:
                        # if pressure is equal to or greater than 1000 mb, the leading 1 is dropped
                        if parts_3[0] == "0":
                            parts_3.insert(0, "1")
                            aircraft_static_air_pressure = "".join(parts_3)
                            aircraft_static_air_pressure = float(aircraft_static_air_pressure) / 10
                        else:
                            aircraft_static_air_pressure = float(parts[3]) / 10

                        aircraft_static_air_pressure_inhg = round(
                            aircraft_static_air_pressure / 33.8639, 2
                        )

                    if str(parts[4]) == "/////":
                        aircraft_geopotential_height = None
                        aircraft_geopotential_height_ft = None
                    else:
                        aircraft_geopotential_height = int(int(parts[4]) / 1)
                        aircraft_geopotential_height_ft = round(
                            float(aircraft_geopotential_height) * 3.2808
                        )

                    if str(parts[5]) == "////":
                        extrapolated_surface_pressure = None
                        extrapolated_surface_pressure_inhg = None
                    else:
                        # if pressure is equal to or greater than 1000 mb, the leading 1 is dropped
                        if parts_5[0] == "0":
                            parts_5.insert(0, "1")
                            extrapolated_surface_pressure = "".join(parts_5)
                            extrapolated_surface_pressure = (
                                float(extrapolated_surface_pressure) / 10
                            )
                        else:
                            extrapolated_surface_pressure = float(parts[5]) / 10

                        extrapolated_surface_pressure_inhg = round(
                            extrapolated_surface_pressure / 33.8639, 2
                        )

                    # change to D-value if aircraft static pressure is lower than 550.0 mb
                    if (
                        aircraft_static_air_pressure is not None
                        and extrapolated_surface_pressure is not None
                        and aircraft_static_air_pressure < 550.0
                    ):
                        extrapolated_surface_pressure = None
                        extrapolated_surface_pressure_inhg = None
                        d_value = int("".join(parts_5[1:]))
                        d_value_ft = round(float(d_value) * 3.2808)
                    else:
                        d_value = None
                        d_value_ft = None

                    if str(parts[6]) == "////":
                        air_temperature = None
                        air_temperature_f = None
                    else:
                        if parts_6[0] == "-":
                            air_temperature = -abs(float("".join(parts_6[1:]))) / 10
                        else:
                            air_temperature = float("".join(parts_6[1:])) / 10

                        air_temperature_f = round((air_temperature * 1.8) + 32, 1)

                    if str(parts[7]) == "////":
                        dew_point = None
                        dew_point_f = None
                    else:
                        if parts_7[0] == "-":
                            dew_point = -abs(float("".join(parts_7[1:]))) / 10
                        else:
                            dew_point = float("".join(parts_7[1:])) / 10

                        dew_point_f = round((dew_point * 1.8) + 32, 1)

                    if str(parts[8]) == "//////" or str(parts[8]) == "999":
                        wind_direction = None
                        wind_cardinal_direction = None
                    else:
                        wind_direction = int("".join(parts_8[0:3]))
                        directions = [
                            "N",
                            "NNE",
                            "NE",
                            "ENE",
                            "E",
                            "ESE",
                            "SE",
                            "SSE",
                            "S",
                            "SSW",
                            "SW",
                            "WSW",
                            "W",
                            "WNW",
                            "NW",
                            "NNW",
                            "N",
                        ]
                        wind_cardinal_direction = directions[round(float(wind_direction) / 22.5)]

                    if str(parts[8]) == "//////" or str(parts[8]) == "999":
                        wind_speed = None
                        wind_speed_mph = None
                    else:
                        wind_speed = int("".join(parts_8[3:]))
                        wind_speed_mph = round(float(wind_speed) * 1.151)

                    if str(parts[9]) == "///" or str(parts[9]) == "999":
                        peak_wind_speed = None
                        peak_wind_speed_mph = None
                    else:
                        peak_wind_speed = int(parts[9])
                        peak_wind_speed_mph = round(float(peak_wind_speed) * 1.151)

                    if str(parts[10]) == "///" or str(parts[10]) == "999":
                        sfmr_peak_surface_wind_speed = None
                        sfmr_peak_surface_wind_speed_mph = None
                    else:
                        sfmr_peak_surface_wind_speed = int(parts[10])
                        sfmr_peak_surface_wind_speed_mph = round(
                            float(sfmr_peak_surface_wind_speed) * 1.151
                        )

                    if str(parts[11]) == "///" or str(parts[11]) == "999":
                        sfmr_surface_rain_rate = None
                        sfmr_surface_rain_rate_in = None
                    else:
                        sfmr_surface_rain_rate = int(parts[11])
                        sfmr_surface_rain_rate_in = round(float(sfmr_surface_rain_rate) / 25.4, 2)

                    first_flag, second_flag = quality_control_decoder(parts_12[0], parts_12[1])

                    observations_dict = {
                        "observation_time": parts[0],
                        "hour": "".join(parts_0[0:2]),
                        "minute": "".join(parts_0[2:4]),
                        "second": "".join(parts_0[4:]),
                        "coordinates": parts[1] + parts[2],
                        "latitude": parts[1],
                        "longitude": parts[2],
                        "aircraft_static_air_pressure": aircraft_static_air_pressure,
                        "aircraft_static_air_pressure_inhg": aircraft_static_air_pressure_inhg,
                        "aircraft_geopotential_height": aircraft_geopotential_height,
                        "aircraft_geopotential_height_ft": aircraft_geopotential_height_ft,
                        "extrapolated_surface_pressure": extrapolated_surface_pressure,
                        "extrapolated_surface_pressure_inhg": extrapolated_surface_pressure_inhg,
                        "d_value": d_value,
                        "d_value_ft": d_value_ft,
                        "air_temperature": air_temperature,
                        "air_temperature_f": air_temperature_f,
                        "dew_point": dew_point,
                        "dew_point_f": dew_point_f,
                        "wind_direction": wind_direction,
                        "wind_cardinal_direction": wind_cardinal_direction,
                        "wind_speed": wind_speed,
                        "wind_speed_mph": wind_speed_mph,
                        "peak_wind_speed": peak_wind_speed,
                        "peak_wind_speed_mph": peak_wind_speed_mph,
                        "sfmr_peak_surface_wind_speed": sfmr_peak_surface_wind_speed,
                        "sfmr_peak_surface_wind_speed_mph": sfmr_peak_surface_wind_speed_mph,
                        "sfmr_surface_rain_rate": sfmr_surface_rain_rate,
                        "sfmr_surface_rain_rate_in": sfmr_surface_rain_rate_in,
                        "quality_control_flags": "".join(parts_12),
                        "first_flag_decoded": first_flag,
                        "second_flag_decoded": second_flag,
                    }

                    observations.append(observations_dict)

        hdob["observations"] = observations

        # skip training missions
        if hdob["mission_number"] != "":
            existing_storm = session.query(Storm).filter_by(name=hdob["storm_name"]).first()

            if existing_storm is None:
                insert_storm(hdob)

            storm_id = session.query(Storm).filter_by(name=hdob["storm_name"]).first().id

            existing_mission = (
                session.query(Mission)
                .filter_by(
                    callsign=hdob["callsign"],
                    mission_number=hdob["mission_number"],
                    storm_id=storm_id,
                )
                .first()
            )

            if existing_mission is None:
                insert_mission(storm_id, hdob)

            mission_id = (
                session.query(Mission)
                .filter_by(
                    callsign=hdob["callsign"],
                    mission_number=hdob["mission_number"],
                    storm_id=storm_id,
                )
                .first()
                .id
            )

            existing_high_density_observation = (
                session.query(HighDensityObservation)
                .filter_by(
                    mission_id=mission_id,
                    observation_number=hdob["observation_number"],
                    transmitted=hdob["transmitted"],
                )
                .first()
            )

            if existing_high_density_observation is None:
                insert_high_density_observation(mission_id, hdob)

                high_density_observation_id = (
                    session.query(HighDensityObservation)
                    .filter_by(
                        mission_id=mission_id,
                        observation_number=hdob["observation_number"],
                        transmitted=hdob["transmitted"],
                    )
                    .first()
                    .id
                )

                insert_observation(high_density_observation_id, hdob)

        observations.clear()
        hdob.clear()


def collect_hdobs(year=datetime.now().year, db_insert=False):
    url = f"https://www.nhc.noaa.gov/archive/recon/{year}/AHONT1"
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

    if not os.path.exists(HDOBS_DIR):
        os.makedirs(HDOBS_DIR)

    for hdob in hdobs:
        if not os.path.exists(f"{HDOBS_DIR}/{hdob}"):
            response = requests.get(f"{url}/{hdob}")

            if response.status_code == 200:
                with open(f"{HDOBS_DIR}/{hdob}", "w") as f:
                    f.write(response.text)

    os.remove("recon.html")

    if db_insert is True:
        insert_hdobs()
