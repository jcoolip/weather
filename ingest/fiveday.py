###############################################
# ingest/fiveday.py
# This is a test script to make sure the weather pipeline works
# without having to use flask run or restarting web service.
#
# Retrieves lat lon of city
# Retrieves five day forecast for city
# Retrieves current weather for city
###############################################


from fetch import five_day_forecast, get_current_weather2
from geocode import geocode_city

# Mapping of weather codes to descriptive text
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

# Geocode the city to get its latitude and longitude
city = geocode_city("Beckley")

# Get the five-day forecast using the city's coordinates
five_day = five_day_forecast(city["latitude"], city["longitude"])

# Get the current weather using the city's coordinates
current = get_current_weather2(city["latitude"], city["longitude"])

# Print the current weather data
print(current)
