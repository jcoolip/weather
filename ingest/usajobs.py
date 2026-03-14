import json
import os

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://data.usajobs.gov/api/search"

HEADERS = {
    "Host": "data.usajobs.gov",
    "User-Agent": os.getenv("USAJOBS_EMAIL"),
    "Authorization-Key": os.getenv("USAJOBS_API_KEY"),
}
PARAMS_LIST = [
    {"keyword": "it jobs"},
    {"keyword": "python"},
    {"keyword": "engineering jobs"},
    {"keyword": "data analyst"},
    {"keyword": "scrub tech"},
]
# PARAMS = {"Keyword": "data analyst", "ResultsPerPage": 25}

DB_URL = os.getenv("DATABASE_URL")


def fetch_jobs(cur):
    rows_added = 0
    for p in PARAMS_LIST:
        r = requests.get(API_URL, headers=HEADERS, params=p, timeout=10)
        r.raise_for_status()
        f = f"1{p.get('keyword')}.json"
        jobs = r.json()["SearchResult"]["SearchResultItems"]
        with open(f, "w") as json_file:
            json.dump(jobs, json_file, indent=4)

        for item in jobs:
            job = item["MatchedObjectDescriptor"]
            external_id = item["MatchedObjectId"]
            company_id = upsert_company(cur, job["OrganizationName"])
            location = job.get("PositionLocation", [{}])[0]

            location_id = upsert_location(cur, location)

            _, inserted = insert_job(cur, external_id, job, company_id, location_id)
            rows_added += inserted

    return rows_added


def get_connection():
    return psycopg2.connect(DB_URL)


def upsert_company(cur, name):
    cur.execute(
        """
        INSERT INTO companies (name)
        VALUES (%s)
        ON CONFLICT (name)
        DO UPDATE SET name = companies.name
        RETURNING id;
    """,
        (name,),
    )
    return cur.fetchone()[0]


def upsert_location(cur, location_obj):

    location = location_obj.get("CityName", "")
    parts = [p.strip() for p in location.split(",")]

    state = parts[-1] if len(parts) >= 1 else ""
    city = parts[-2] if len(parts) >= 2 else ""
    site = ", ".join(parts[:-2]) if len(parts) > 2 else ""

    country = location_obj.get("CountryCode") or ""

    cur.execute(
        """
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        ON CONFLICT (city, state, country)
        DO UPDATE SET city = locations.city
        RETURNING id;
    """,
        (city, state, country),
    )

    return cur.fetchone()[0]


def insert_job(cur, external_id, job, company_id, location_id):
    r = job.get("PositionRemuneration", [{}])[0]
    salary_min = float(r.get("MinimumRange")) if r.get("MinimumRange") else None
    salary_max = float(r.get("MaximumRange")) if r.get("MaximumRange") else None

    cur.execute(
        """
        INSERT INTO jobs (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            qualifications,
            source,
            source_url,
            salary_min,
            salary_max
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET
            last_seen = now(),
            title = EXCLUDED.title,
            qualifications = EXCLUDED.qualifications,
            description_raw = EXCLUDED.description_raw,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            is_active = TRUE
        RETURNING id, (xmax = 0) AS inserted;
    """,
        (
            external_id,
            company_id,
            location_id,
            job["PositionTitle"],
            job["UserArea"]["Details"]["AgencyMarketingStatement"],
            job["QualificationSummary"],
            "USAJobs",
            job["PositionURI"],
            salary_min,
            salary_max,
        ),
    )
    job_id, inserted = cur.fetchone()

    return job_id, int(inserted)


def main():

    conn = get_connection()
    cur = conn.cursor()

    rows_added = fetch_jobs(cur)

    conn.commit()
    cur.close()
    conn.close()

    print(f"USAJobs added {rows_added}")


if __name__ == "__main__":
    main()
