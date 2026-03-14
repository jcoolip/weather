import json
import os
import re
import traceback

import psycopg2
import requests
from dotenv import load_dotenv
from requests.exceptions import HTTPError, Timeout

debug = False

load_dotenv()

## TODO:  unescape html to store into db as raw.
# import html

# title = html.unescape(job["title"])
# description_raw = html.unescape(job["description"])
# company = html.unescape(company)


API_URL = os.getenv("JOBICY_URL")
DB_URL = os.getenv("DATABASE_URL")
SOURCE = "Jobicy"


# remove ["results"] and this is static
def fetch_jobs():
    try:
        r = requests.get(API_URL, timeout=15)
        r.raise_for_status()
        return r.json()["jobs"]
    except Timeout as e:
        return {
            "error": f"Request to {API_URL} timed out after 15s",
            "details": str(e),
        }
    except HTTPError as e:
        return {
            "error": "Jobicy 503 temp. unavail - HTTP error",
            "details": str(e),
        }
    except Exception as e:
        return {
            "error": "An unexpected error occurred.",
            "details": traceback.format_exc(),
        }


def test(cur, jobs):
    for job in jobs:
        print(job)


def normalize_results(cur, jobs):
    rows_added = 0
    for job in jobs:
        # id,
        # company_id = upsert_company(cur, company)
        # location_id,
        title = job["jobTitle"]
        description_raw = job["jobDescription"]
        employment_type = job.get("jobType")
        experience_level = None
        salary_min = job.get("salaryMin")
        salary_max = job.get("salaryMax")
        salary_currency = job.get("salaryCurrency")
        source = SOURCE
        source_url = job["url"]
        # first_seen
        # last_seen
        # is_active
        work_mode = None
        external_id = job["id"]
        qualifications = None
        salary_freq = job.get("salaryPeriod") or None
        salary_raw = None
        tags = None
        salary_predicted = None
        benefits = None
        responsibilities = None
        industry_id = None
        published_date = job["pubDate"]

        area = job.get("location", {}).get("area", [])
        if not area:
            country = "Unknown"
        elif "," in area[0]:
            country = area[0].split(",")[0].strip()
        else:
            country = area[0]
        city = None
        state = None

        company = job["companyName"]
        company_id = upsert_company(cur, company)
        source = SOURCE
        location_id = upsert_location(cur, city, state, country)
        job_id, inserted = insert_job(
            cur,
            company_id,
            location_id,
            title,
            description_raw,
            employment_type,
            experience_level,
            salary_min,
            salary_max,
            salary_currency,
            source,
            source_url,
            work_mode,
            external_id,
            qualifications,
            salary_freq,
            salary_raw,
            tags,
            salary_predicted,
            benefits,
            responsibilities,
            industry_id,
            published_date,
        )
        rows_added += inserted
        fetch_dbskills(cur, job_id, description_raw)
    return rows_added


def insert_job(
    cur,
    company_id,
    location_id,
    title,
    description_raw,
    employment_type,
    experience_level,
    salary_min,
    salary_max,
    salary_currency,
    source,
    source_url,
    work_mode,
    external_id,
    qualifications,
    salary_freq,
    salary_raw,
    tags,
    salary_predicted,
    benefits,
    responsibilities,
    industry_id,
    published_date,
):
    cur.execute(
        """
        INSERT INTO jobs (
        company_id,
        location_id,
        title,
        description_raw,
        employment_type,
        salary_min,
        salary_max,
        salary_currency,
        source,
        source_url ,
        work_mode,
        external_id,
        qualifications,
        salary_freq,
        salary_raw ,
        tags,
        salary_predicted,
        benefits,
        responsibilities,
        industry_id,
        published_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET
            last_seen = now(),
            source_url = EXCLUDED.source_url,
            title =EXCLUDED.title,
            description_raw =EXCLUDED.description_raw,
            employment_type =EXCLUDED.employment_type,
            salary_min =EXCLUDED.salary_min,
            salary_max =EXCLUDED.salary_max,
            salary_currency =EXCLUDED.salary_currency,
            work_mode =EXCLUDED.work_mode,
            qualifications =EXCLUDED.qualifications,
            salary_freq =EXCLUDED.salary_freq,
            salary_raw  =EXCLUDED.salary_raw,
            tags =EXCLUDED.tags,
            salary_predicted =EXCLUDED.salary_predicted,
            benefits =EXCLUDED.benefits,
            responsibilities =EXCLUDED.responsibilities,
            industry_id =EXCLUDED.industry_id,
            published_date =EXCLUDED.published_date
        RETURNING id, (xmax = 0) AS inserted;
    """,
        (
            company_id,
            location_id,
            title,
            description_raw,
            employment_type,
            salary_min,
            salary_max,
            salary_currency,
            source,
            source_url,
            work_mode,
            external_id,
            qualifications,
            salary_freq,
            salary_raw,
            tags,
            salary_predicted,
            benefits,
            responsibilities,
            industry_id,
            published_date,
        ),
    )
    job_id, inserted = cur.fetchone()

    return job_id, int(inserted)


def db_close(cur, conn):
    if debug:
        conn.rollback()
    else:
        conn.commit()
    cur.close()
    conn.close()


def db_open():
    conn = get_connection()
    cur = conn.cursor()
    return conn, cur


## TODO rename this. more like check_jobskills()
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
    with open("logs/JobicyTests.json", "w") as f:
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
    ## save api call results in json
    save_json(jobs)

    ## establish db connection and cursor
    conn, cur = db_open()

    ## assign job variables from retrieved job,
    ## upsert company, location,
    ## insert job
    ## scan job description for known skills and insert
    rows_added = normalize_results(cur, jobs)
    # test(cur, jobs)
    ## commit our sql
    ## close our cursor and connection
    db_close(cur, conn)

    print(f"Jobicy added {rows_added}")


if __name__ == "__main__":
    main()
