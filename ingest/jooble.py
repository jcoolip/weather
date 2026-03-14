import json
import os
import re

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

debug = False

API_KEY = "96d5229d-435e-41ea-aad5-21a3b46309f4"
API_URL = f"https://jooble.org/api/{API_KEY}"

BODY_LIST = [
    {"keywords": "it", "location": "USA"},
    {"keywords": "python", "location": "USA"},
    {"keywords": "data+analyst", "location": "USA"},
    {"keywords": "scrub+tech", "location": "WV"},
]

# body = {
#     "keywords": "it",
#     "location": "USA"
# }

headers = {"Content-Type": "application/json"}


def fetch_jobs(cur):
    inserted = 0
    for query in BODY_LIST:
        r = requests.post(API_URL, json=query, headers=headers, timeout=20)
        r.raise_for_status()
        jobs = r.json()["jobs"]
        save_json(jobs, query)
        inserted += assign_job_info(cur, jobs)

    return inserted


DB_URL = os.getenv("DATABASE_URL")


def save_json(jobs, x):
    file = x.get("keywords")
    with open(f"logs/{file}.json", "w") as f:
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


def insert_job(
    cur,
    external_id,
    company_id,
    location_id,
    title,
    description_raw,
    source,
    source_url,
    sraw,
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
            salary_raw
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET
            last_seen = now(),
            source_url = EXCLUDED.source_url,
            title = EXCLUDED.title,
            description_raw = EXCLUDED.description_raw,
            salary_raw = EXCLUDED.salary_raw
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
            sraw,
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


# sometimes remotive (maybe others?) uses this:
# en dash (Unicode U+2013)"–" instead of usual hyphen "-"
salary_pattern = re.compile(
    r"(?P<currency>[$€£])?\s*(?P<min>\d[\d,\.]*k?)\s*(?:[-–]\s*(?P<max>\d[\d,\.]*k?))?\s*(?:/?\s*(?P<freq>hour|hr|year|yr|month|mo|day))?",
    re.IGNORECASE,
)


def parse_salary(text):
    m = salary_pattern.search(text)
    if not m:
        return None, None, None, None

    currency = m.group("currency")
    smin = m.group("min")
    smax = m.group("max")
    freq = m.group("freq")

    def normalize(v):
        if not v:
            return ""
        v = v.replace(",", "").lower()
        if "k" in v:
            return float(v.replace("k", "")) * 1000
        return float(v)

    return normalize(smin), normalize(smax), freq, currency


def assign_job_info(cur, jobs):
    rows_added = 0
    for job in jobs:
        title = job["title"]
        external_id = job["id"]
        description_raw = job["snippet"]
        salary_raw = job["salary"]
        # smin, smax, freq, curr = parse_salary(text)  ## TODO
        loc = job.get("location")
        loc = loc.split(",")
        if len(loc) == 0:
            city = ""
            state = ""
        elif len(loc) == 1:
            city = ""
            state = loc
        else:
            city = loc[0]
            state = loc[1].strip()
        country = ""
        company = job.get("company")
        source = "Jooble"
        source_url = job.get("link")

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
            salary_raw,
        )
        rows_added += inserted
        fetch_dbskills(cur, job_id, description_raw)
        # print(f"{external_id}:::{title}:::{salary_raw}:::{company}:::{state}:::{description_raw}::::::::")
    return rows_added


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


def main():

    ## establish db connection and cursor
    conn, cur = db_open()

    ## api call to retrieve and store jobs
    rows_added = fetch_jobs(cur)
    ## save api call results in json
    # save_json(jobs)

    ## assign job variables from retrieved job,
    ## upsert company, location,
    ## insert job
    ## scan job description for known skills and insert
    # rows_added = assign_job_info(cur, jobs)

    ## commit our sql
    ## close our cursor and connection
    db_close(cur, conn)

    print(f"Jooble added {rows_added}")


if __name__ == "__main__":
    main()
