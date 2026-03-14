import os
import psycopg2

INDUSTRY_RULES = {

    "Technology": [
        "microsoft", "google", "alphabet", "apple", "meta", "facebook",
        "amazon web services", "aws", "oracle", "ibm", "intel", "amd",
        "nvidia", "cisco", "salesforce", "adobe", "servicenow",
        "snowflake", "palantir", "databricks", "atlassian",
        "dell", "hp", "lenovo", "software", "developer", "engineer",
        "ai", "machine learning", "data", "cloud", "devops"
    ],

    "Healthcare": [
        "pfizer", "johnson & johnson", "moderna", "merck", "abbvie",
        "bristol myers", "unitedhealth", "anthem", "humana",
        "cvs", "walgreens", "kaiser", "mayo clinic",
        "cleveland clinic", "hca healthcare", "tenet",
        "cigna", "blue cross", "medtronic", "abbott",
        "hospital", "clinical", "medical", "patient",
        "pharma", "health", "therapy", "nurse"
    ],

    "Finance": [
        "jpmorgan", "chase", "bank of america", "wells fargo",
        "citibank", "goldman", "morgan stanley",
        "blackrock", "state street", "american express",
        "capital one", "visa", "mastercard", "paypal",
        "schwab", "fidelity", "prudential", "metlife",
        "aig", "allstate", "progressive",
        "finance", "bank", "investment", "insurance",
        "accounting", "audit", "tax"
    ],

    "Manufacturing": [
        "caterpillar", "john deere", "3m", "ge", "general electric",
        "honeywell", "boeing", "lockheed", "raytheon",
        "northrop", "siemens", "bosch", "emerson",
        "whirlpool", "procter", "pepsico", "coca cola",
        "nestle", "tyson", "plant", "production",
        "manufacturing", "assembly", "industrial", "machinist"
    ],

    "Construction": [
        "bechtel", "fluor", "kiewit", "jacobs",
        "aecom", "turner construction", "skanska",
        "lennar", "dr horton", "pulte",
        "quanta", "mastec", "black & veatch",
        "construction", "electrician", "plumber",
        "hvac", "carpenter", "laborer"
    ],

    "Retail": [
        "walmart", "target", "costco", "home depot",
        "lowes", "best buy", "kroger", "albertsons",
        "amazon", "ebay", "etsy",
        "nike", "adidas", "starbucks",
        "mcdonald", "chipotle", "dominos",
        "retail", "store", "customer", "sales associate"
    ],

    "Transportation": [
        "ups", "fedex", "usps", "dhl",
        "union pacific", "norfolk southern", "csx",
        "delta", "united airlines", "american airlines",
        "southwest", "uber", "lyft",
        "jb hunt", "schneider", "xpo",
        "logistics", "transport", "driver", "trucking"
    ],

    "Energy": [
        "exxon", "exxonmobil", "chevron", "shell",
        "bp", "conoco", "marathon",
        "valero", "nextera", "duke energy",
        "dominion", "southern company",
        "pg&e", "tesla energy",
        "halliburton", "schlumberger", "baker hughes",
        "oil", "gas", "energy", "utility", "power"
    ],

    "Government": [
        "us army", "us navy", "air force",
        "department of defense", "dod",
        "va", "veterans affairs",
        "irs", "nasa", "fbi",
        "state of", "city of", "county of",
        "usajobs", "government", "public sector"
    ],

    "Education": [
        "university", "college", "school district",
        "community college", "state university",
        "harvard", "mit", "stanford",
        "coursera", "edx",
        "teacher", "professor", "education", "campus"
    ],

    "Other": []
}

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def score_text(text, weight):

    scores = {k: 0 for k in INDUSTRY_RULES}

    if not text:
        return scores

    text = text.lower()

    for industry, keywords in INDUSTRY_RULES.items():
        for k in keywords:
            if k in text:
                scores[industry] += weight

    return scores

def merge_scores(total, new):

    for k in total:
        total[k] += new[k]

    return total

def classify_industry(title, desc, company, skills):

    total = {k: 0 for k in INDUSTRY_RULES}

    total = merge_scores(
        total,
        score_text(title, 1)
    )

    total = merge_scores(
        total,
        score_text(desc, 1)
    )

    total = merge_scores(
        total,
        score_text(company, 3)
    )

    total = merge_scores(
        total,
        score_text(skills, 2)
    )

    best = max(total, key=total.get)

    if total[best] == 0:
        return "Other"

    return best

def classify_jobs():

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    j.id,
                    j.title,
                    j.description_raw,
                    c.name,
                    STRING_AGG(s.name, ' ') AS skills
                FROM jobs j
                LEFT JOIN companies c ON c.id = j.company_id
                LEFT JOIN job_skills js ON js.job_id = j.id
                LEFT JOIN skills s ON s.id = js.skill_id
                WHERE j.industry_id IS NULL
                GROUP BY j.id, j.title, j.description_raw, c.name
            """)

            rows = cur.fetchall()

            for job_id, title, desc, company, skills in rows:

                industry = classify_industry(
                    title,
                    desc,
                    company,
                    skills
                )

                cur.execute(
                    "SELECT id FROM industries WHERE name=%s",
                    (industry,)
                )

                row = cur.fetchone()[0]
                if not row:
                    print("Missing industry:", industry)
                    continue
                
                industry_id = row[0] if isinstance(row, tuple) else row

                cur.execute(
                    "UPDATE jobs SET industry_id=%s WHERE id=%s",
                    (industry_id, job_id)
                )

        conn.commit()

def main():

    classify_jobs()

if __name__ == "__main__":
    main()