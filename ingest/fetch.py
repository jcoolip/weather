"""
Fetch weather data from the Open-Meteo API in json format
prepared for find.html to display

"""

from datetime import date, datetime, timedelta

import requests

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

# WMO code: [description, long_description, map_icon_designation]
WMO_WEATHER = {
    0: {
        "name": "Clear",
        "description": "Clear sky",
        "image": "Sunny.gif",
        "accent": "#CB9152",
    },

    1: {
        "name": "Mainly Clear",
        "description": "Mainly clear",
        "image": "Sunny.gif",
        "accent": "#CB9152",
    },

    2: {
        "name": "Partly Cloudy",
        "description": "Partly cloudy",
        "image": "Partly-Cloudy.gif",
        "accent": "#CB9152",
    },

    3: {
        "name": "Overcast",
        "description": "Overcast",
        "image": "Cloudy.gif",
        "accent": "#CCCCCC",
    },

    45: {
        "name": "Fog",
        "description": "Foggy",
        "image": "Fog.gif",
        "accent": "#CCCCCC",
    },
    48: {
        "name": "Fog",
        "description": "Depositing rime fog",
        "image": "Fog.gif",
        "accent": "#CCCCCC",
    },
    51: {
        "name": "Drizzle",
        "description": "Light drizzle",
        "image": "Showers.gif",
        "accent": "#92F5FA",
    },
    53: {
        "name": "Drizzle",
        "description": "Moderate drizzle",
        "image": "Showers.gif",
        "accent": "#92F5FA",
    },
    55: {
        "name": "Drizzle",
        "description": "Dense drizzle",
        "image": "Wintry-Mix.gif",
        "accent": "#92F5FA",
    },
    56: {
        "name": "Freezing Drizzle",
        "description": "Light freezing drizzle",
        "image": "Wintry-Mix.gif",
        "accent": "#92F5FA",
    },
    57: {
        "name": "Freezing Drizzle",
        "description": "Dense freezing drizzle",
        "image": "Wintry-Mix.gif",
        "accent": "#92F5FA",
    },
    61: {
        "name": "Rain",
        "description": "Slight rain",
        "image": "Showers.gif",
        "accent": "#92F5FA",
    },
    63: {
        "name": "Rain",
        "description": "Moderate rain",
        "image": "Rain.gif",
        "accent": "#92F5FA",
    },
    65: {
        "name": "Rain",
        "description": "Heavy rain",
        "image": "Rain.gif",
        "accent": "#92F5FA",
    },
    66: {
        "name": "Freezing Rain",
        "description": "Light freezing rain",
        "image": "Freezing-Rain.gif",
        "accent": "#ffffff",
    },
    67: {
        "name": "Freezing Rain",
        "description": "Heavy freezing rain",
        "image": "Freezing-Rain.gif",
        "accent": "#ffffff",
    },
    71: {
        "name": "Snow",
        "description": "Slight snow fall",
        "image": "Light-Snow.gif",
        "accent": "#ffffff",
    },
    73: {
        "name": "Snow",
        "description": "Moderate snow fall",
        "image": "Light-Snow.gif",
        "accent": "#ffffff",
    },
    75: {
        "name": "Snow",
        "description": "Heavy snow fall",
        "image": "Heavy-Snow.gif",
        "accent": "#ffffff",
    },
    77: {
        "name": "Snow",
        "description": "Snow grains falling",
        "image": "Light-Snow.gif",
        "accent": "#ffffff",
    },
    80: {
        "name": "Rain Showers",
        "description": "Slight rain showers",
        "image": "Showers.gif",
        "accent": "#92F5FA",
    },
    81: {
        "name": "Rain Showers",
        "description": "Moderate rain showers",
        "image": "Rain.gif",
        "accent": "#92F5FA",
    },
    82: {
        "name": "Rain Showers",
        "description": "Violent rain showers",
        "image": "Rain.gif",
        "accent": "#92F5FA",
    },
    85: {
        "name": "Snow Showers",
        "description": "Slight snow showers",
        "image": "Light-Snow.gif",
        "accent": "#ffffff",
    },
    86: {
        "name": "Snow Showers",
        "description": "Heavy snow showers",
        "image": "Heavy-Snow.gif",
        "accent": "#ffffff",
    },
    95: {
        "name": "Thunderstorms",
        "description": "Thunderstorm",
        "image": "Thunderstorms.gif",
        "accent": "#f2b64a",
    },
    96: {
        "name": "Thunderstorms",
        "description": "Thunderstorm with slight hail",
        "image": "Thunderstorms.gif",
        "accent": "#f2b64a",
    },
    99: {
        "name": "Thunderstorms",
        "description": "Thunderstorm with heavy hail",
        "image": "Thunderstorms.gif",
        "accent": "#f2b64a",
    }
}

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
            "weather_code": WMO_WEATHER.get(daily["weather_code"][i], "Unknown"),
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
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    r_json = response.json()
    print(r_json)

    current = r_json["current"]
    current_units = r_json["current_units"]

    weather_code = current["weather_code"]
    weather = WMO_WEATHER.get(weather_code,
        {
            "name": "Unknown",
            "description": "Unknown weather conditions",
            "image": "Cloudy.gif",
            "accent": "#CCCCCC",
        },
    ).copy()

    current_hour = datetime.now().hour

    if current_hour >= 21 or current_hour < 4:
        weather["image"] = "Night.gif"

    row = {
        "date": date,
        "weather_code": current["weather_code"],
        "weather": weather,
        "temp": current["temperature_2m"],
        "feels_like": current["apparent_temperature"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "wind_gust": current["wind_gusts_10m"],
        "wind_dir": deg_to_compass(current["wind_direction_10m"]),
        "visibility": round(current["visibility"] / 1609.344, 1),
        "surface_pressure": current["surface_pressure"],
        "generationtime_ms": round(r_json["generationtime_ms"],2),
        "temp_unit": current_units["apparent_temperature"],
        "relative_humidity_unit": current_units["relative_humidity_2m"],
        "precipitation_unit": current_units["precipitation"],
        "wind_speed_unit": current_units["wind_speed_10m"],
        "surface_pressure_unit": current_units["surface_pressure"],
        "visibility_unit": current_units["visibility"]
    }

    return row
