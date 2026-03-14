"""
Utilities to geocode a city name using the Open-Meteo geocoding API.

This module provides a single helper `geocode_city` which returns a small
dictionary containing the original typed name plus canonical location
information (name, latitude, longitude, timezone) returned by the API.
"""

import requests


def geocode_city(city_name="Beckley", results=3):
    """Geocode a city name to location metadata.

    Args:
        city_name (str): User-typed city name. Defaults to "New York City"

    Returns:
        dict: A dictionary with keys `typed_name`, `name`, `latitude`,
              `longitude`, and `timezone` suitable for downstream use.

    Raises:
        ValueError: If the API response doesn't include a `results` entry.
    """

    # Build the geocoding API endpoint and request parameters. We ask for
    # a single (most relevant) result by setting count=1.
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        "name": city_name,
        "count": results
    }
    #print(results)
    # Make the HTTP GET request to the geocoding API.
    response = requests.get(url, params=params)

    # Parse JSON body only on successful response; other statuses will
    # leave `data` undefined and cause the subsequent check to fail.
    if response.status_code == 200:
        data = response.json()

    # If there are no results in the response, raise a meaningful error so
    # callers can handle the lookup failure explicitly.
    if "results" not in data:
        raise ValueError(f"City '{city_name}' not found in geocoding API.")
    
    # Use the first result returned by the API (most relevant match).
    result = data['results']

    loc_list = {}

    for i, item in enumerate(result):
        # print(f"{i}: {item['name']}, {item['admin1']} - {item['country']}")

        loc_list[i] = {
            "name": item['name'],
            "state": item['admin1'],
            "country": item['country']
        }

    return loc_list

# loc1 = geocode_city(city_name="houston", results=5)
# loc2 = geocode_city(city_name="Toronto", results=5)
# loc3 = geocode_city(city_name="miami", results=5)
# loc4 = geocode_city(city_name="salt lake", results=5)

# print(loc1[0])
# print(loc2[0])
# print(loc3[0])
# print(loc4[0])