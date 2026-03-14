import os
import psycopg2
from dotenv import load_dotenv
import re

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")


def fetch_dbskills(cur):
    ## very important WHERE statement here... ##
    cur.execute(
        """
        SELECT j.id, j.tags
        FROM jobs as j
        WHERE source = 'remotive';
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

    for id, skill in jobs:
        split_tags = skill.split(",")
        for tag in split_tags:
            if tag != "":
                for x,y in skills:
                    if tag.lower() in x.lower():
                        tag_skill_on_job(cur, id, y, 1)

def tag_skill_on_job(cur, job, skill, weight):
    cur.execute(
        """
        INSERT INTO job_skills(job_id, skill_id, weight)
        VALUES (%s, %s, %s)
        ON CONFLICT (job_id, skill_id)
        DO UPDATE SET weight = EXCLUDED.weight
    """,
        (job, skill, 1),
    )

def get_conn():
    return psycopg2.connect(DB_URL)

def main():
    conn = get_conn()
    cur = conn.cursor()
    fetch_dbskills(cur)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
