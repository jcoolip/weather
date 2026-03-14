import os
import requests
import psycopg2
import json
import re
from dotenv import load_dotenv

debug = False

load_dotenv()

API_URL = "https://jsearch.p.rapidapi.com/search"

querystring = {"query":"data analyst United states","page":"1","num_pages":"1","country":"us","date_posted":"all","work_from_home":"true"}

headers = {
	"x-rapidapi-key": "c8e82f8d37msh3470d768e679d12p12fc0djsn0cac4e2107d1",
	"x-rapidapi-host": "jsearch.p.rapidapi.com"
}

DB_URL = os.getenv("DATABASE_URL")

def fetch_jobs():
    r = requests.get(API_URL, headers=headers, params=querystring, timeout=30)
    r.raise_for_status()
    return r.json()['data']

def save_json(jobs):
    with open("jsearch_results.json", "w") as f:
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

def insert_job(cur, external_id, company_id, location_id, 
               title, description_raw, source, source_url, 
               salary_min, salary_max, salary_freq,
               employment_type, qualifications, benefits, 
               responsibilities, is_remote):
  
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
            salary_min,
            salary_max,
            salary_freq,
            employment_type,
            qualifications,
            benefits,
            responsibilities,
            work_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET 
            last_seen = now(),
            source_url = EXCLUDED.source_url,
            title = EXCLUDED.title,
            description_raw = EXCLUDED.description_raw,
            employment_type = EXCLUDED.employment_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            qualifications = EXCLUDED.qualifications,
            benefits = EXCLUDED.benefits,
            responsibilities = EXCLUDED.responsibilities,
            work_mode = EXCLUDED.work_mode
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
            salary_min,
            salary_max,
            salary_freq,
            employment_type,
            qualifications,
            benefits,
            responsibilities,
            is_remote
        ),
    )
    job_id, inserted = cur.fetchone()

    return job_id, int(inserted)

def assign_job_info(cur, jobs):
    for job in jobs:
        title = job['job_title']
        external_id = job['job_id']
        description_raw = job['job_description']
        salary_min = job['job_min_salary']
        salary_max = job['job_max_salary']
        salary_freq = job['job_salary_period']
        country = job['job_country'] or "US"
        state = job['job_state'] or "Unknown"
        city = job['job_city'] or "Unknown"
        company = job['employer_name']
        source = "JSearch"
        source_url = job['job_apply_link']
        employment_type = job['job_employment_type']
        if job['job_highlights'].get("Qualifications"):
            qualifications = ', '.join(job['job_highlights'].get("Qualifications"))
        else:
            qualifications = None        
        if job['job_highlights'].get("Benefits"):
            benefits = ', '.join(job['job_highlights'].get("Benefits"))
        else:
            benefits = None
        if job['job_highlights'].get("Responsibilities"):
            responsibilities = ', '.join(job['job_highlights'].get("Responsibilities"))
        else:
            responsibilities = None
        is_remote = job['job_is_remote']
        if is_remote and is_remote == 'true':
            is_remote = "remote"
        else:
            is_remote = "unknown"
        company_id = upsert_company(cur, company)
        location_id = upsert_location(cur, city, state, country)
        job_id, inserted = insert_job(cur, external_id, company_id, location_id, title, description_raw, source, source_url, salary_min, salary_max, salary_freq, employment_type, qualifications, benefits, responsibilities, is_remote)
        fetch_dbskills(cur, job_id, description_raw)

        return inserted

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

def db_close(cur, conn):
    if not debug:
        conn.commit()
    cur.close()
    conn.close()

def db_open():
    conn = get_connection()
    cur = conn.cursor()
    return conn, cur

def main():
    rows_added = 0
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
    rows_added = assign_job_info(cur, jobs)
    
    ## commit our sql 
    ## close our cursor and connection
    db_close(cur, conn)

    print(f"Jsearch added {rows_added}")

if __name__ == "__main__":
    main()
