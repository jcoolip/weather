import os

import psycopg2
from dotenv import load_dotenv
from flask import Flask, render_template, request
from livereload import Server

from ingest.fetch import five_day_forecast, get_current_weather2
from ingest.geocode2 import geocode_city

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


def deg_to_compass(deg):
    try:
        d = float(deg)
    except (TypeError, ValueError):
        return None
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int((d / 45.0) + 0.5) % 8
    return directions[idx]

import requests


def reverse_geocode(latitude, longitude):
    url = "https://nominatim.openstreetmap.org/reverse"

    params = {
        "lat": latitude,
        "lon": longitude,
        "format": "jsonv2",
        "addressdetails": 1,
        "zoom": 10,
    }

    headers = {
        "User-Agent": "jcullop-weather/1.0"
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()
    address = data.get("address", {})

    name = (
        address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("municipality")
        or address.get("county")
        or "Current Location"
    )

    state = (
        address.get("state_code")
        or address.get("state")
        or ""
    )

    return {
        "name": name,
        "state": state,
        "country": address.get("country"),
        "latitude": latitude,
        "longitude": longitude,
        "population": None,
        "elevation": None,
    }

def get_elevation(latitude, longitude):
    response = requests.get(
        "https://api.open-meteo.com/v1/elevation",
        params={
            "latitude": latitude,
            "longitude": longitude,
        },
        timeout=10,
    )
    response.raise_for_status()

    elevation_meters = response.json()["elevation"][0]

    if elevation_meters is None:
        return None

    return round(elevation_meters * 3.28084)


load_dotenv()

### DEV_MODE=1 : development
### DEV_MODE=0 : production
### Missing : defaults to development
dev_mode = os.getenv("DEV_MODE", "1") == "1"

app = Flask(__name__)


@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/health")
def health():
    return {"status": "awesome sauce"}

@app.route("/embed/current")
def embed_current():
    loc = geocode_city("25801")
    cur_weather = get_current_weather2(
        lat=loc["latitude"],
        lon=loc["longitude"],
    )

    return render_template(
        "embed_current.html",
        loc=loc,
        cur_weather=cur_weather,
    )




DEFAULT_LATITUDE = 40.71
DEFAULT_LONGITUDE = -74.00


@app.route("/")
def index():
    q = request.args.get("q", "Beckley").strip()

    cur_weather = None
    loc = None
    error = None

    try:
        loc = geocode_city(q)

        if "error" in loc:
            error = loc["error"]
            loc = None
        else:
            cur_weather = get_current_weather2(
                loc["latitude"],
                loc["longitude"],
            )

    except requests.RequestException:
        error = "Unable to contact the weather service."

    except Exception as exc:
        app.logger.exception("Weather lookup failed")
        error = "Unable to retrieve weather information."

    return render_template(
        "find.html",
        q=q,
        loc=loc,
        cur_weather=cur_weather,
        error=error,
    )



    ### get user submitted input
    ### default to Beckley if first load
    ### TODO: ask for location/save pref somehow.
    ###       i think its cookies. mmm
#    q = request.args.get("q", "25801")
#    cur_weather = None
#    loc = None

#    if q:
#        try:
#            loc = geocode_city(q)
#            if isinstance(loc, dict) and "error" in loc:
#                cur_weather = None
#                forecast = None
#            else:
#                cur_weather = (
#                    get_current_weather2(loc["latitude"], loc["longitude"]) or None
#                )
#                forecast = five_day_forecast(loc["latitude"], loc["longitude"]) or None
#        except Exception as e:
#            cur_weather = None
#            forecast = None

#    return render_template(
#        "find.html",
#        q=q,
#        cur_weather=cur_weather,
#        forecast=forecast,
#        loc=loc,
#    )


#@app.route("/")
#def home():
#    return render_template("index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5500))
    app.run(host="0.0.0.0", port=port, debug=True)
    # if dev_mode:
    #     port = int(os.environ.get("PORT", 5500))  # dev port for livereload
    #     server = Server(app.wsgi_app)
    #     # watch templates and static CSS
    #     server.watch("templates/")
    #     server.watch("static/css/")
    #     # optional: watch Python files and reload server
    #     server.watch("*.py")
    #     server.serve(host="0.0.0.0", port=port, debug=True)
    # else:
    #     port = int(os.environ.get("PORT", 8000))
    #     app.run(host="0.0.0.0", port=port, debug=True)
