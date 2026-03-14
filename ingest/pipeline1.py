import json
import os
import re
import traceback

import psycopg2
import requests
from dotenv import load_dotenv
from requests.exceptions import Timeout

debug = True
# SOURCE = "Adzuna"

load_dotenv()

API_URL = "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=42b6f469&app_key=65b227d0c211e8eb15d4817c030afc82&results_per_page=50&category=it-jobs&content-type=application/"


DB_URL = os.getenv("DATABASE_URL")


# remove ["results"] and this is static
def fetch_jobs():
    try:
        r = requests.get(API_URL, timeout=10)
        r.raise_for_status()
        return r.json()["results"]
    except Timeout as e:
        return {
            "error": f"Request to {API_URL} timed out after 10s",
            "details": str(e),
        }
    except Exception as e:
        return {
            "error": "An unexpected error occurred.",
            "details": traceback.format_exc(),
        }


# normalizing()
def assign_job_info(cur, jobs):
    rows_added = 0
    for job in jobs:
        title = job["title"]
        external_id = job["id"]
        description_raw = job["description"]
        salary_min = job.get("salary_min")
        salary_max = job.get("salary_max")
        salary_predicted = job.get("salary_is_predicted")
        # salary_min = job["salary_min"]
        # salary_max = job["salary_max"]
        # salary_predicted = job["salary_is_predicted"]
        area = job.get("location", {}).get("area", [])

        country = area[0] if len(area) > 0 else "Unknown"
        state = area[1] if len(area) > 1 else "Unknown"
        city = area[2] if len(area) > 2 else ""
        # if len(job["location"]["area"]) == 0:
        #     country = "Unknown"
        #     state = "Unknown"
        #     city = ""
        # else:
        #     country = job["location"]["area"][0]
        #     state = job["location"]["area"][1]
        #     city = ""
        company = job.get("company", {}).get("display_name", "Unknown")
        # company = job["company"]["display_name"]
        source = SOURCE
        source_url = job["redirect_url"]
        employment_type = job.get("contract_time") or job.get("contract_type") or None
        company_id = upsert_company(cur, company)
        location_id = upsert_location(cur, city, state, country)
        job_id, inserted = insert_job(
            cur,
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            source,
            source_url,
            salary_min,
            salary_max,
            salary_predicted,
            employment_type,
        )
        rows_added += inserted
        fetch_dbskills(cur, job_id, description_raw)
    return rows_added


# needs to be static
# every source should insert to entire table
# whether it has value or None
def insert_job(
    cur,
    external_id,
    company_id,
    location_id,
    title,
    description_raw,
    source,
    source_url,
    salary_min,
    salary_max,
    salary_predicted,
    employment_type,
):
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
            employment_type,
            salary_min,
            salary_max,
            salary_currency,
            salary_predicted
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET
            last_seen = now(),
            source_url = EXCLUDED.source_url,
            title = EXCLUDED.title,
            description_raw = EXCLUDED.description_raw,
            employment_type = EXCLUDED.employment_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            salary_currency = EXCLUDED.salary_currency,
            salary_predicted = EXCLUDED.salary_predicted
        RETURNING id, (xmax = 0) AS inserted;
    """,
        (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            source,
            source_url,
            employment_type,
            salary_min,
            salary_max,
            "$",
            salary_predicted,
        ),
    )
    job_id, inserted = cur.fetchone()

    return job_id, int(inserted)


def db_close(cur, conn):
    if not debug:
        conn.commit()
    cur.close()
    conn.close()


def db_open():
    conn = get_connection()
    cur = conn.cursor()
    return conn, cur


def fetch_dbskills(cur, job_id, job_desc):
    # populate our skills from table
    cur.execute(
        """
        SELECT s.normalized_name, s.id
        FROM skills as s;
    """,
    )
    skills = cur.fetchall()

    compiled_skills = [
        (s_id, s_name, re.compile(rf"\b{re.escape(s_name)}\b", re.IGNORECASE))
        for s_name, s_id in skills
    ]

    if job_desc == "":
        return
    job_desc = job_desc.lower()
    for s_id, s_name, s_patt in compiled_skills:
        weight = len(s_patt.findall(job_desc))
        if weight:
            tag_skill_on_job(cur, job_id, s_id, weight)


def tag_skill_on_job(cur, job, skill, weight):
    cur.execute(
        """
        INSERT INTO job_skills(job_id, skill_id, weight)
        VALUES (%s, %s, %s)
        ON CONFLICT (job_id, skill_id)
        DO UPDATE SET weight = EXCLUDED.weight
    """,
        (job, skill, weight),
    )


def save_json(jobs):
    with open(f"logs/{SOURCE}Tests.json", "w") as f:
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


def upsert_location(cur, city, state, country):

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


def main():

    ## api call to retrieve and store jobs
    jobs = fetch_jobs()
    if isinstance(jobs, dict) and "error" in jobs:
        print(jobs)
        return
    ## save api call results in json
    save_json(jobs)

    ## establish db connection and cursor
    conn, cur = db_open()

    ## assign job variables from retrieved job,
    ## upsert company, location,
    ## insert job
    ## scan job description for known skills and insert
    rows_added = assign_job_info(cur, jobs)

    ## commit our sql
    ## close our cursor and connection
    db_close(cur, conn)

    print(f"Adzuna added {rows_added}")


if __name__ == "__main__":
    main()
