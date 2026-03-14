import os
import psycopg2
from dotenv import load_dotenv
import re

debug = True

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

def fetch_dbskills(cur):
    ## very important WHERE statement here... ##
    cur.execute(
        """
        SELECT j.id, j.description_raw
        FROM jobs as j
        WHERE source like 'adzuna';
        """,
    )
    jobs = cur.fetchall()

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

    for j_id, j_desc in jobs:
        text = (j_desc or "").lower()

        for s_id, s_name, s_patt in compiled_skills:
            weight = len(s_patt.findall(text))

            if weight:
                tag_skill_on_job(cur, j_id, s_id, weight)


def is_remote(cur):
    ## very important WHERE statement here... ##
    cur.execute(
        """
        SELECT j.id, j.title, j.description_raw, l.id, l.city
        FROM jobs as j
        join locations as l
        on j.location_id = l.id
        WHERE source like 'adzuna';
        """,
    )
    jobs = cur.fetchall()

    for j_id, j_title, j_desc, l_id, l_city in jobs:
        find_onsite = len(re.findall(r"\bonsite\b", l_city, re.IGNORECASE))
        find_onsite += len(re.findall(r"\bonsite\b", j_desc, re.IGNORECASE))
        find_hybrid = len(re.findall(r"\bhybrid\b", l_city, re.IGNORECASE))
        find_hybrid += len(re.findall(r"\bhybrid\b", j_desc, re.IGNORECASE))
        find_remote = len(re.findall(r"\bremote\b", l_city, re.IGNORECASE))
        find_remote += len(re.findall(r"\bremote\b", j_desc, re.IGNORECASE))
        
        if find_onsite > find_remote and find_onsite > find_hybrid:
            workplace = "onsite"
        elif find_remote > find_hybrid:
            workplace = "remote"
        else:
            workplace = "hybrid"

        if find_remote + find_hybrid + find_onsite < 1:
            workplace = "unknown"

        cur.execute(
            """
            UPDATE jobs
            SET work_mode = %s
            WHERE id = %s
            """,
            (workplace, j_id),
        )

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


def get_conn():
    return psycopg2.connect(DB_URL)

def open_db():
    conn = get_conn()
    cur = conn.cursor()
    return cur, conn

def close_db(cur, conn):
    if not debug:
        conn.commit()
    cur.close()
    conn.close()

def main():
    cur, conn = open_db()

    fetch_dbskills(cur)
    is_remote(cur)

    close_db(cur, conn)

if __name__ == "__main__":
    main()
