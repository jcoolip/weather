import os
from dotenv import load_dotenv
import re

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

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


# def main():
#     conn, cur = db_open()

#     fetch_dbskills(cur)
#     is_remote(cur)

#     db_close(cur, conn)


# if __name__ == "__main__":
#     main()
