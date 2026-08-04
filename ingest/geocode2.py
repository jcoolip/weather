"""
Utilities to geocode a city name using the Open-Meteo geocoding API.

This module provides a single helper `geocode_city` which returns a small
dictionary containing the original typed name plus canonical location
information (name, latitude, longitude, timezone) returned by the API.
"""

import requests


def geocode_city(city_name="Beckley"):
    url = "https://geocoding-api.open-meteo.com/v1/search"

    params = {
        "name": city_name,
        "count": 1,
        "language": "en",
        "format": "json",
    }

    response = requests.get(
        url,
        params=params,
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])

    if not results:
        return {
            "error": f'Location "{city_name}" was not found.'
        }

    result = results[0]

    return {
        "typed_name": city_name,
        "name": result.get("name", city_name),
        "latitude": result["latitude"],
        "longitude": result["longitude"],
        "timezone": result.get("timezone"),
        "elevation": result.get("elevation"),
        "population": result.get("population"),
        "country": result.get("country"),
        "state": result.get("admin1"),
    }