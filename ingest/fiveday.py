import pandas as pd
from fetch import deg_to_compass, five_day_forecast, get_current_weather2
from geocode import geocode_city

weather_code_map = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle:",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light Freezing Drizzle",
    57: "Dense Freezing Drizzle:",
    61: "Slight Rain",
    63: "Moderate Rain",
    65: "Heavy Rain",
    66: "Light Freezing Rain",
    67: "Heavy Freezing Rain",
    71: "Slight Snow fall",
    73: "Moderate Snow fall",
    75: "Heavy Snow fall",
    77: "Snow grains",
    80: "Slight Rain showers",
    81: "Moderate Rain showers",
    82: "Violent Rain showers",
    85: "Slight Snow showers",
    86: "Heavy Snow showers",
    95: "Slight Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


city = geocode_city("Beckley")
# print(city)
five_day = five_day_forecast(city["latitude"], city["longitude"])
current = get_current_weather2(city["latitude"], city["longitude"])


print(current)

# daily = five_day["daily"]

# forecast = []

# for i in range(len(daily["time"])):
#     row = {
#         "date": daily["time"][i],
#         "weather_code": weather_code_map.get(daily["weather_code"][i], "Unknown"),
#         "temp_max": daily["temperature_2m_max"][i],
#         "temp_min": daily["temperature_2m_min"][i],
#         "apparent_max": daily["apparent_temperature_max"][i],
#         "apparent_min": daily["apparent_temperature_min"][i],
#         "sunrise": daily["sunrise"][i],
#         "sunset": daily["sunset"][i],
#         "uv": daily["uv_index_max"][i],
#         "rain": daily["rain_sum"][i],
#         "showers": daily["showers_sum"][i],
#         "snow": daily["snowfall_sum"][i],
#         "precip": daily["precipitation_sum"][i],
#         "precip_prob": daily["precipitation_probability_max"][i],
#         "wind_speed": daily["wind_speed_10m_max"][i],
#         "wind_gust": daily["wind_gusts_10m_max"][i],
#         "wind_dir": deg_to_compass(daily["wind_direction_10m_dominant"][i]),
#     }

#     forecast.append(row)

# print(forecast)
