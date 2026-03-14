"""
Utilities to geocode a city name using the Open-Meteo geocoding API.

This module provides a single helper `geocode_city` which returns a small
dictionary containing the original typed name plus canonical location
information (name, latitude, longitude, timezone) returned by the API.
"""

import requests


def geocode_city(city_name="Beckley"):
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
        "count": 1
    }

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
    result = data["results"][0]

    # Return a compact dictionary with both the original typed name and the
    # canonical fields the rest of the application expects.
    return {
        "typed_name": city_name,
        "name": result["name"],
        "latitude": result["latitude"],
        "longitude": result["longitude"],
        "timezone": result["timezone"],
        "elevation": result["elevation"],
        "population": result["population"],
        "country": result["country"],
        "state": result["admin1"]
    }