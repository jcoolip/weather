import os
import requests
import psycopg2
import json
import re
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://remotive.com/api/remote-jobs"

PARAMS = {"Category": "data", "limit": 10}

DB_URL = os.getenv("DATABASE_URL")

# sometimes remotive uses this: en dash (Unicode U+2013)"–" instead of usual hyphen "-"
salary_pattern = re.compile(
    r'(?P<currency>[$€£])?\s*(?P<min>\d[\d,\.]*k?)\s*(?:[-–]\s*(?P<max>\d[\d,\.]*k?))?\s*(?:/?\s*(?P<freq>hour|hr|year|yr|month|mo|day))?',
    re.IGNORECASE
)

def parse_salary(text):
    m = salary_pattern.search(text)
    if not m:
        return None

    currency = m.group("currency")
    smin = m.group("min")
    smax = m.group("max")
    freq = m.group("freq")

    def normalize(v):
        if not v:
            return None
        v = v.replace(",", "").lower()
        if "k" in v:
            return float(v.replace("k", "")) * 1000
        return float(v)

    return normalize(smin), normalize(smax), freq, currency

def fetch_jobs():
    r = requests.get(API_URL, params=PARAMS, timeout=10)
    r.raise_for_status()
    return r.json()["jobs"]

def save_json(jobs):
    with open("remotiveTests.json", "w") as f:
        json.dump(jobs, f, indent=4)

def get_connection():
    return psycopg2.connect(DB_URL)

def upsert_company(cur, name):
    cur.execute(
        """
        INSERT INTO companies (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id;
    """,
        (name,),
    )
    return cur.fetchone()[0]

def upsert_location(cur, location_obj):
    city = ""
    state =  ""
    country = location_obj

    cur.execute(
        """
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        ON CONFLICT (city, state, country)
        DO UPDATE SET city = EXCLUDED.city
        RETURNING id;
    """,
        (city, state, country),
    )

    return cur.fetchone()[0]

def insert_job(cur, external_id, item, company_id, location_id):
    salary = item["salary"] or ""
    if salary != "":
        salary_min, salary_max, salary_freq, salary_currency = parse_salary(salary)
    else:
        salary_min = None
        salary_max = None
        salary_freq = None
        salary_currency = None

    tags = item["tags"]
    tags_row = ",".join(tags)


    cur.execute(
        """
        INSERT INTO jobs (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            source,
            source_url,
            work_mode,
            employment_type,
            salary_min,
            salary_max,
            salary_freq,
            salary_currency,
            salary_raw,
            tags
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET 
            last_seen = now(),
            work_mode = EXCLUDED.work_mode,
            employment_type = EXCLUDED.employment_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            salary_freq = EXCLUDED.salary_freq,
            salary_currency = EXCLUDED.salary_currency,
            salary_raw = EXCLUDED.salary_raw,
            tags = EXCLUDED.tags
        RETURNING id, (xmax = 0) AS inserted;
    """,
        (
            external_id,
            company_id,
            location_id,
            item["title"],
            item["description"],
            "Remotive",
            item["url"],
            "remote",
            item["job_type"],
            salary_min,
            salary_max,
            salary_freq,
            salary_currency,
            salary,
            tags_row
        ),
    )
    job_id, inserted = cur.fetchone()

    return job_id, int(inserted)

def main():

    rows_added = 0

    jobs = fetch_jobs()
    conn = get_connection()
    cur = conn.cursor()
    save_json(jobs)

    for item in jobs:
        external_id = item["id"]
        company_id = upsert_company(cur, item["company_name"])
        location = item["candidate_required_location"]
        location_id = upsert_location(cur, location)
        _, inserted = insert_job(cur, external_id, item, company_id, location_id)
        rows_added += inserted

    conn.commit()
    cur.close()
    conn.close()

    print(f"Remotive added {rows_added}")

if __name__ == "__main__":
    main()
