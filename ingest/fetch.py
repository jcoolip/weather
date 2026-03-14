"""
Fetch weather data from the Open-Meteo archive API and return it as
Pandas DataFrames for hourly and daily series.

This module uses `openmeteo_requests` (a client wrapper around the API
response format) and `requests_cache` plus `retry_requests` to make the
calls resilient and cache responses locally.
"""
from zoneinfo import ZoneInfo
import openmeteo_requests
import json
import pandas as pd
import requests_cache
from retry_requests import retry
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, date

def normalize_hourly_dataframe(
		df: pd.DataFrame,
		user_timezone: str
	) -> pd.DataFrame:
    """
    Normalize hourly dataframe timestamps for production use.

    - Assumes df["date"] is UTC
    - Converts to user timezone
    - Adds canonical time fields
    """

    # Ensure UTC-aware
    df["utc_time"] = pd.to_datetime(df["date"], utc=True)

    # Convert to user timezone
    tz = ZoneInfo(user_timezone)
    df["local_time"] = df["utc_time"].dt.tz_convert(tz)

    # Derived, app-safe fields
    df["local_date"] = df["local_time"].dt.date.astype(str)
    df["local_hour"] = df["local_time"].dt.hour
    df["timezone"] = user_timezone

    # Stable forecast index (0 = first hour returned)
    df["forecast_hour_index"] = range(len(df))

    # Optional: drop raw date if you want to enforce discipline
    # df = df.drop(columns=["date"])

    return df

def get_forecast_hours(lat = None, lon = None, timezone=None, start_date = None, end_date = None, forecast_hours = 12):
	"""Fetch hourly weather for a single location.

	Args:
		lat (float): Latitude of location. Defaults to a hard-coded value.
		lon (float): Longitude of location. Defaults to a hard-coded value.
		forecast_hours (int): Number of hours to forecast starting from now. Defaults to 12.

	Returns:
		tuple: (hourly_dataframe, daily_dataframe, start_date, end_date)

	Notes:
		- The order of variables in `hourly` and `daily` must match the
		  extraction order below because the Open-Meteo client exposes
		  variables as positional indices.
		- The function currently processes only the first response
		  (`responses[0]`) — it can be extended to loop multiple
		  locations/models if needed.
	"""
	now = datetime.now().strftime("%m-%d %H:%M")

	# Use sensible defaults when callers omit parameters.
	if start_date is None:
		start_date = now
	if end_date is None:
		end_date = now

		
	# Configure a cached requests session and wrap it with retry logic to
	# avoid transient network failures. The cache directory is '.cache'.
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)


	
	# Build API parameters: list all desired hourly and daily variables.
	url = "https://api.open-meteo.com/v1/forecast"
	params = {
		"latitude": lat,
		"longitude": lon,
		"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "visibility", "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "temperature_80m", "soil_temperature_6cm", "soil_moisture_1_to_3cm", "freezing_level_height", "uv_index"],
		"wind_speed_unit": "mph",
		"temperature_unit": "fahrenheit",
		"precipitation_unit": "inch",
		"forecast_hours": forecast_hours,
		"timezone": "auto"
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process only the first response for this single-location helper.
	response = responses[0]

	# --- HOURLY ---
	# Extract hourly variables in the same ordered manner as requested
	# (positional indices must match the `hourly` list above).
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
	hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
	hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
	hourly_rain = hourly.Variables(4).ValuesAsNumpy()
	hourly_snowfall = hourly.Variables(5).ValuesAsNumpy()
	hourly_snow_depth = hourly.Variables(6).ValuesAsNumpy()
	hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
	hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(9).ValuesAsNumpy()
	hourly_wind_gusts_10m = hourly.Variables(10).ValuesAsNumpy()
	hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
	hourly_soil_temperature_0_to_7cm = hourly.Variables(12).ValuesAsNumpy()
	hourly_soil_moisture_0_to_7cm = hourly.Variables(13).ValuesAsNumpy()

	# Build the hourly datetime index using the client's Time/TimeEnd/Interval
	# information and create a dictionary of series that will become a DataFrame.
	hourly_data = {"date": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s"),
		end =  pd.to_datetime(hourly.TimeEnd(), unit = "s"),
		freq = pd.Timedelta(seconds = hourly.Interval()),
		inclusive = "left"
	)}

	hourly_data["temperature_2m"] = hourly_temperature_2m
	hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
	hourly_data["dew_point_2m"] = hourly_dew_point_2m
	hourly_data["apparent_temperature"] = hourly_apparent_temperature
	hourly_data["rain"] = hourly_rain
	hourly_data["snowfall"] = hourly_snowfall
	hourly_data["snow_depth"] = hourly_snow_depth
	hourly_data["surface_pressure"] = hourly_surface_pressure
	hourly_data["cloud_cover"] = hourly_cloud_cover
	hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
	hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
	hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
	hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
	hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm

	hourly_dataframe = pd.DataFrame(data = hourly_data)

	# Normalize timestamps for production
	if timezone is None:
		timezone = "UTC"  # fallback safety

	hourly_dataframe = normalize_hourly_dataframe(
		hourly_dataframe,
		user_timezone=timezone
	)
	print(f"timezone: {timezone}")
	print(f"hourly_dataframe timezone column: {hourly_dataframe['timezone'].iloc[0]}")

	# Return both DataFrames and echo the (possibly defaulted) date range.
	return hourly_dataframe, None, start_date, end_date

def get_forecast_days(lat = None, lon = None, start_date = None, end_date = None, days_ahead = 5):
	"""Fetch daily weather for a single location.

	Args:
		lat (float): Latitude of location. Defaults to a hard-coded value.
		lon (float): Longitude of location. Defaults to a hard-coded value.
		start_date (str): YYYY-MM-DD start date. Defaults to today.
		end_date (str): YYYY-MM-DD end date. Defaults to today.

	Returns:
		tuple: (hourly_dataframe, daily_dataframe, start_date, end_date)

	Notes:
		- The order of variables in `hourly` and `daily` must match the
		  extraction order below because the Open-Meteo client exposes
		  variables as positional indices.
		- The function currently processes only the first response
		  (`responses[0]`) — it can be extended to loop multiple
		  locations/models if needed.
	"""

	# Compute start_date and end_date. If `start_date` is omitted use today.
	# `days_ahead` takes precedence over an explicit `end_date`.
	now_date = date.today()
	if start_date is None:
		start_dt = now_date
	else:
		start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()

	if days_ahead is not None:
		# coerce and validate
		days = int(days_ahead)
		if days < 0:
			raise ValueError("days_ahead must be a non-negative integer")
		end_dt = start_dt + timedelta(days=days)
	elif end_date is not None:
		end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
	else:
		end_dt = start_dt

	# format for the API
	start_date = start_dt.strftime("%Y-%m-%d")
	end_date = end_dt.strftime("%Y-%m-%d")
	print(f"{start_date} to {end_date} ({(end_dt - start_dt).days} days)")

		
	# Configure a cached requests session and wrap it with retry logic to
	# avoid transient network failures. The cache directory is '.cache'.
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)


	
	# Build API parameters: list all desired hourly and daily variables.
	url = "https://api.open-meteo.com/v1/forecast"
	params = {
		"latitude": lat,
		"longitude": lon,
		"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min", "sunrise", "uv_index_max", "rain_sum", "showers_sum", "snowfall_sum", "precipitation_sum", "precipitation_probability_max", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant"],
		"wind_speed_unit": "mph",
		"temperature_unit": "fahrenheit",
		"precipitation_unit": "inch",
		"start_date": start_date,
		"end_date": end_date,
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation: {response.Elevation()} m asl")
	print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

	# Process daily data. The order of variables needs to be the same as requested.
	daily = response.Daily()
	daily_weather_code = daily.Variables(0).ValuesAsNumpy()
	daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
	daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
	daily_apparent_temperature_max = daily.Variables(3).ValuesAsNumpy()
	daily_apparent_temperature_min = daily.Variables(4).ValuesAsNumpy()
	daily_sunrise = daily.Variables(5).ValuesInt64AsNumpy()
	daily_uv_index_max = daily.Variables(6).ValuesAsNumpy()
	daily_rain_sum = daily.Variables(7).ValuesAsNumpy()
	daily_showers_sum = daily.Variables(8).ValuesAsNumpy()
	daily_snowfall_sum = daily.Variables(9).ValuesAsNumpy()
	daily_precipitation_sum = daily.Variables(10).ValuesAsNumpy()
	daily_precipitation_probability_max = daily.Variables(11).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(12).ValuesAsNumpy()
	daily_wind_gusts_10m_max = daily.Variables(13).ValuesAsNumpy()
	daily_wind_direction_10m_dominant = daily.Variables(14).ValuesAsNumpy()

	daily_data = {"date": pd.date_range(
		start = pd.to_datetime(daily.Time(), unit = "s"),
		end =  pd.to_datetime(daily.TimeEnd(), unit = "s"),
		freq = pd.Timedelta(seconds = daily.Interval()),
		inclusive = "left"
	)}

	daily_data["weather_code"] = daily_weather_code
	daily_data["temperature_2m_max"] = daily_temperature_2m_max
	daily_data["temperature_2m_min"] = daily_temperature_2m_min
	daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
	daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
	daily_data["sunrise"] = daily_sunrise
	daily_data["uv_index_max"] = daily_uv_index_max
	daily_data["rain_sum"] = daily_rain_sum
	daily_data["showers_sum"] = daily_showers_sum
	daily_data["snowfall_sum"] = daily_snowfall_sum
	daily_data["precipitation_sum"] = daily_precipitation_sum
	daily_data["precipitation_probability_max"] = daily_precipitation_probability_max
	daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
	daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
	daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

	daily_dataframe = pd.DataFrame(data = daily_data)
	print("\nDaily data\n", daily_dataframe)

	hourly_dataframe = None

	# Return both DataFrames and echo the (possibly defaulted) date range.
	return hourly_dataframe, daily_dataframe, start_date, end_date

def get_current_weather(lat = None, lon = None, forecast_hours=None):
	"""Fetch current weather for a single location."""
	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	start_date = end_date = datetime.now().strftime("%b %d")

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://api.open-meteo.com/v1/forecast"
	params = {
		"latitude": lat,
		"longitude": lon,
		"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "visibility", "evapotranspiration", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "temperature_80m", "soil_temperature_6cm", "soil_moisture_1_to_3cm", "freezing_level_height", "uv_index"],
		"current": ["temperature_2m", "apparent_temperature", "weather_code"],
		"wind_speed_unit": "mph",
		"temperature_unit": "fahrenheit",
		"precipitation_unit": "inch",
		"forecast_hours": forecast_hours,
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation: {response.Elevation()} m asl")
	print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

	# Process current data. The order of variables needs to be the same as requested.
	current = response.Current()
	current_temperature_2m = current.Variables(0).Value()
	current_apparent_temperature = current.Variables(1).Value()
	current_weather_code = current.Variables(2).Value()

	print(f"\nCurrent time: {current.Time()}")
	print(f"Current temperature_2m: {current_temperature_2m}")
	print(f"Current apparent_temperature: {current_apparent_temperature}")
	print(f"Current weather_code: {current_weather_code}")

	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
	hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
	hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
	hourly_precipitation_probability = hourly.Variables(4).ValuesAsNumpy()
	hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
	hourly_rain = hourly.Variables(6).ValuesAsNumpy()
	hourly_showers = hourly.Variables(7).ValuesAsNumpy()
	hourly_snowfall = hourly.Variables(8).ValuesAsNumpy()
	hourly_snow_depth = hourly.Variables(9).ValuesAsNumpy()
	hourly_weather_code = hourly.Variables(10).ValuesAsNumpy()
	hourly_pressure_msl = hourly.Variables(11).ValuesAsNumpy()
	hourly_surface_pressure = hourly.Variables(12).ValuesAsNumpy()
	hourly_cloud_cover = hourly.Variables(13).ValuesAsNumpy()
	hourly_visibility = hourly.Variables(14).ValuesAsNumpy()
	hourly_evapotranspiration = hourly.Variables(15).ValuesAsNumpy()
	hourly_et0_fao_evapotranspiration = hourly.Variables(16).ValuesAsNumpy()
	hourly_vapour_pressure_deficit = hourly.Variables(17).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(18).ValuesAsNumpy()
	hourly_wind_direction_10m = hourly.Variables(19).ValuesAsNumpy()
	hourly_wind_gusts_10m = hourly.Variables(20).ValuesAsNumpy()
	hourly_temperature_80m = hourly.Variables(21).ValuesAsNumpy()
	hourly_soil_temperature_6cm = hourly.Variables(22).ValuesAsNumpy()
	hourly_soil_moisture_1_to_3cm = hourly.Variables(23).ValuesAsNumpy()
	hourly_freezing_level_height = hourly.Variables(24).ValuesAsNumpy()
	hourly_uv_index = hourly.Variables(25).ValuesAsNumpy()

	hourly_data = {"date": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s"),
		end =  pd.to_datetime(hourly.TimeEnd(), unit = "s"),
		freq = pd.Timedelta(seconds = hourly.Interval()),
		inclusive = "left"
	)}

	hourly_data["temperature_2m"] = hourly_temperature_2m
	hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
	hourly_data["dew_point_2m"] = hourly_dew_point_2m
	hourly_data["apparent_temperature"] = hourly_apparent_temperature
	hourly_data["precipitation_probability"] = hourly_precipitation_probability
	hourly_data["precipitation"] = hourly_precipitation
	hourly_data["rain"] = hourly_rain
	hourly_data["showers"] = hourly_showers
	hourly_data["snowfall"] = hourly_snowfall
	hourly_data["snow_depth"] = hourly_snow_depth
	hourly_data["weather_code"] = hourly_weather_code
	hourly_data["pressure_msl"] = hourly_pressure_msl
	hourly_data["surface_pressure"] = hourly_surface_pressure
	hourly_data["cloud_cover"] = hourly_cloud_cover
	hourly_data["visibility"] = hourly_visibility
	hourly_data["evapotranspiration"] = hourly_evapotranspiration
	hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
	hourly_data["vapour_pressure_deficit"] = hourly_vapour_pressure_deficit
	hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
	hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
	hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
	hourly_data["temperature_80m"] = hourly_temperature_80m
	hourly_data["soil_temperature_6cm"] = hourly_soil_temperature_6cm
	hourly_data["soil_moisture_1_to_3cm"] = hourly_soil_moisture_1_to_3cm
	hourly_data["freezing_level_height"] = hourly_freezing_level_height
	hourly_data["uv_index"] = hourly_uv_index

	hourly_dataframe = pd.DataFrame(data = hourly_data)
	print("\nHourly data\n", hourly_dataframe)
	return hourly_dataframe, None, start_date, end_date