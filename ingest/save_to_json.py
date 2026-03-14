import json
from datetime import datetime

def save_to_json(df_loc, df_hourly, df_daily, geocoded_city, real_start_date, real_end_date, forecast_hours=None):
    city = geocoded_city['name']
    now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    try:
        df_loc.to_json(f"ingest/json/loc_{city.replace(' ', '_')}_{df_loc['state'].iloc[0]}.json", orient = "records", date_format = "iso", indent=2)
        print(f"Geocoded city json saved to ingest/json/loc_{city.replace(' ', '_')}_{df_loc['state'].iloc[0]}.json")
        
        if forecast_hours is None:
            df_daily.to_json(f"ingest/json/{geocoded_city['name']}_{df_loc['state'].iloc[0]}_{real_start_date}_to_{real_end_date}.json", orient = "records", date_format = "iso", indent=2)
            print(f"Current json saved to ingest/json/{geocoded_city['name']}_{df_loc['state'].iloc[0]}_{real_start_date}_to_{real_end_date}.json")
        else:
            df_hourly.to_json(f"ingest/json/{geocoded_city['name']}_{df_loc['state'].iloc[0]}_{now}_{forecast_hours}_hrs.json", orient = "records", date_format = "iso", indent=2)
            print(f"Hourly json saved to ingest/json/{geocoded_city['name']}_{df_loc['state'].iloc[0]}_{now}_{forecast_hours}_hrs.json")
    except Exception as e:
        print(f"Error saving to JSON: {e}")