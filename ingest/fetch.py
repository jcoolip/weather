"""
Fetch weather data from the Open-Meteo API in json format
prepared for find.html to display

"""

from datetime import date, datetime, timedelta

import requests

## TODO:  images for conditions
## using emojis right now. maybe that's good enough
# 0,1: clear
# 2,3: cloudy
# 45,48: fog
# 51,53,55: light rain
# 56,57,66,67,77: freezing rain
# 61,63: slight rain
# 65: heavy rain
# 71,73: Slight Snow fall
# 75: Heavy Snow fall
# 80,81: Rain showers
# 82: Violent Rain showers
# 85: Slight Snow showers
# 86: Heavy Snow showers
# 95: Slight Thunderstorm
# 96,99: Thunderstorm with slight hail

weather_code_map = {
    0: "☀️",
    1: "🌤️",
    2: "⛅",
    3: "⛅",
    45: "🌫️",
    48: "🌫️",
    51: "🌦️",
    53: "🌦️",
    55: "🌧️",
    56: "🌨️",
    57: "🌨️",
    61: "🌧️",
    63: "🌧️",
    65: "🌧️",
    66: "🌧️❄️",
    67: "🌧️❄️",
    71: "🌨️",
    73: "🌨️",
    75: "🌨️",
    77: "🌨️",
    80: "☔",
    81: "☔",
    82: "☔",
    85: "🌨️",
    86: "🌨️",
    95: "🌩️",
    96: "🌩️❄️",
    99: "🌩️❄️",
}
# weather_code_map = {
#     0: "Clear sky ☀️",
#     1: "Mainly clear 🌤️",
#     2: "Partly cloudy ⛅",
#     3: "Overcast ⛅",
#     45: "Fog 🌫️",
#     48: "Depositing rime fog 🌫️",
#     51: "Light drizzle 🌦️",
#     53: "Moderate drizzle 🌦️",
#     55: "Dense drizzle 🌧️",
#     56: "Light Freezing Drizzle 🌨️",
#     57: "Dense Freezing Drizzle 🌨️",
#     61: "Slight Rain 🌧️",
#     63: "Moderate Rain 🌧️",
#     65: "Heavy Rain 🌧️",
#     66: "Light Freezing Rain 🌧️❄️",
#     67: "Heavy Freezing Rain 🌧️❄️",
#     71: "Slight Snow fall 🌨️",
#     73: "Moderate Snow fall 🌨️",
#     75: "Heavy Snow fall 🌨️",
#     77: "Snow grains 🌨️",
#     80: "Slight Rain showers ☔",
#     81: "Moderate Rain showers ☔",
#     82: "Violent Rain showers ☔",
#     85: "Slight Snow showers 🌨️",
#     86: "Heavy Snow showers 🌨️",
#     95: "Slight Thunderstorm 🌩️",
#     96: "Thunderstorm with slight hail 🌩️❄️",
#     99: "Thunderstorm with heavy hail 🌩️❄️",
# }
uv_index_map = {
    0: "Low",
    1: "Low",
    2: "Low",
    3: "Moderate",
    4: "Moderate",
    5: "Moderate",
    6: "High",
    7: "High",
    8: "Very High",
    9: "Very High",
    10: "Very High",
    11: "Extreme",
}


def deg_to_compass(deg):
    # openmeteo gives us a number for direction
    # and we need to convert cause i don't know
    # which direction is 42

    try:
        d = float(deg)
    except (TypeError, ValueError):
        return None
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    dir = int((d / 45.0) + 0.5) % 8
    return directions[dir]


def format_day(date_str):
    ### Return formatted day like 'Mon 13th' from 'YYYY-MM-DD'
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    day_num = dt.day

    # Determine ordinal suffix
    if 4 <= day_num <= 20 or 24 <= day_num <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day_num % 10 - 1]

    return dt.strftime(f"%a {day_num}{suffix}")


def five_day_forecast(lat=None, lon=None, start_date=None, end_date=None, days_ahead=5):
    # start_date is always today
    # days_ahead is = 5
    #
    start_date = date.today()
    end_date = start_date + timedelta(days=days_ahead)

    # format for the API
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    # Configure a cached requests session and wrap it with retry logic to
    # avoid transient network failures. The cache directory is '.cache'.
    # cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    # retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    # openmeteo = openmeteo_requests.Client(session=retry_session)

    # Build API parameters: list all desired hourly and daily variables.
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "sunrise",
            "sunset",
            "uv_index_max",
            "rain_sum",
            "showers_sum",
            "snowfall_sum",
            "precipitation_sum",
            "precipitation_probability_max",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant",
        ],
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "start_date": start_date,
        "end_date": end_date,
    }
    # responses = openmeteo.weather_api(url, params=params)
    r_json = requests.get(url, params=params)
    r_json = r_json.json()

    daily = r_json["daily"]

    forecast = []

    for i in range(len(daily["time"])):
        row = {
            "date": format_day(daily["time"][i]),
            "weather_code": weather_code_map.get(daily["weather_code"][i], "Unknown"),
            "temp_max": daily["temperature_2m_max"][i],
            "temp_min": daily["temperature_2m_min"][i],
            "apparent_max": daily["apparent_temperature_max"][i],
            "apparent_min": daily["apparent_temperature_min"][i],
            "sunrise": daily["sunrise"][i],
            "sunset": daily["sunset"][i],
            "uv": daily["uv_index_max"][i],
            "uv_index": uv_index_map.get(int(daily["uv_index_max"][i]), "Unknown"),
            "rain": daily["rain_sum"][i],
            "showers": daily["showers_sum"][i],
            "snow": daily["snowfall_sum"][i],
            "precip": round(daily["precipitation_sum"][i], 2),
            "precip_prob": daily["precipitation_probability_max"][i],
            "wind_speed": daily["wind_speed_10m_max"][i],
            "wind_gust": daily["wind_gusts_10m_max"][i],
            "wind_dir": deg_to_compass(daily["wind_direction_10m_dominant"][i]),
        }

        forecast.append(row)

    ### save json for testing
    # f = "logs/five_day_forecast"
    # with open(f, "w") as file:
    #     json.dump(r_json, file, indent=4)

    ### here lies the fallen
    # # Process first location. Add a for-loop for multiple locations or weather models
    # response = responses[0]
    # print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation: {response.Elevation()} m asl")
    # print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # # Process daily data. The order of variables needs to be the same as requested.
    # daily = response.Daily()
    # daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    # daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    # daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    # daily_apparent_temperature_max = daily.Variables(3).ValuesAsNumpy()
    # daily_apparent_temperature_min = daily.Variables(4).ValuesAsNumpy()
    # daily_sunrise = daily.Variables(5).ValuesInt64AsNumpy()
    # daily_sunset = daily.Variables(6).ValuesInt64AsNumpy()
    # daily_uv_index_max = daily.Variables(7).ValuesAsNumpy()
    # daily_rain_sum = daily.Variables(8).ValuesAsNumpy()
    # daily_showers_sum = daily.Variables(9).ValuesAsNumpy()
    # daily_snowfall_sum = daily.Variables(10).ValuesAsNumpy()
    # daily_precipitation_sum = daily.Variables(11).ValuesAsNumpy()
    # daily_precipitation_probability_max = daily.Variables(12).ValuesAsNumpy()
    # daily_wind_speed_10m_max = daily.Variables(13).ValuesAsNumpy()
    # daily_wind_gusts_10m_max = daily.Variables(14).ValuesAsNumpy()
    # daily_wind_direction_10m_dominant = daily.Variables(15).ValuesAsNumpy()

    # daily_data = {
    #     "date": pd.date_range(
    #         start=pd.to_datetime(daily.Time(), unit="s"),
    #         end=pd.to_datetime(daily.TimeEnd(), unit="s"),
    #         freq=pd.Timedelta(seconds=daily.Interval()),
    #         inclusive="left",
    #     )
    # }

    # daily_data["weather_code"] = daily_weather_code
    # daily_data["temperature_2m_max"] = daily_temperature_2m_max
    # daily_data["temperature_2m_min"] = daily_temperature_2m_min
    # daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
    # daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
    # daily_data["sunrise"] = daily_sunrise
    # daily_data["sunrise"] = daily_sunset
    # daily_data["uv_index_max"] = daily_uv_index_max
    # daily_data["rain_sum"] = daily_rain_sum
    # daily_data["showers_sum"] = daily_showers_sum
    # daily_data["snowfall_sum"] = daily_snowfall_sum
    # daily_data["precipitation_sum"] = daily_precipitation_sum
    # daily_data["precipitation_probability_max"] = daily_precipitation_probability_max
    # daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    # daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
    # daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    # daily_dataframe = pd.DataFrame(data=daily_data)
    # print("\nDaily data\n", daily_dataframe)

    # # hourly_dataframe = None

    # # # Return both DataFrames and echo the (possibly defaulted) datriserisee range.
    # # return hourly_dataframe, daily_dataframe, start_date, end_date
    # return daily_dataframe

    ### retrieving 5 days from api actually returns 6 as you get the current
    ### day as well. list[1:] starts from the second item and keeps the rest.
    ### we do current day weather seperately
    forecast = forecast[1:]
    ### return list of next 5 days from today
    return forecast


def get_current_weather2(lat=None, lon=None, forecast_hours=None):
    date = datetime.now().strftime("%A, %B %d %Y")

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "surface_pressure",
            "visibility",
        ],
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
    }
    r_json = requests.get(url, params=params)
    r_json = r_json.json()
    print(r_json)

    current = r_json["current"]

    row = {
        "date": date,
        "weather_code": weather_code_map.get(current["weather_code"], "Unknown"),
        "temp": current["temperature_2m"],
        "feels_like": current["apparent_temperature"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "wind_gust": current["wind_gusts_10m"],
        "wind_dir": deg_to_compass(current["wind_direction_10m"]),
        "visibility": int(current["visibility"] / 1609),
        "surface_pressure": current["surface_pressure"],
    }

    return row
