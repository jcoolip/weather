import os
from itertools import count

import psycopg2
from dotenv import load_dotenv
from flask import Flask, render_template, request
from livereload import Server
from requests.api import get

from ingest.fetch import get_current_weather
from ingest.geocode2 import geocode_city

load_dotenv()

dev_mode = os.getenv("DEV_MODE", "1") == "1"  # set DEV_MODE=0 for production

app = Flask(__name__)


@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


DB_URL = os.getenv("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DB_URL)


@app.route("/health")
def health():
    return {"status": "awesome sauce"}


@app.route("/find")
def find():

    q = request.args.get("q")
    weather = None

    if q:
        loc = geocode_city(q)
        if isinstance(loc, dict) and "error" in loc:
            weather = {"temp": f"City '{q}' not found in geocoding API."}
        else:
            weather = get_current_weather(loc["latitude"], loc["longitude"]) or None
        # weather = weather.Current()
        # with get_conn() as conn:
        #     with conn.cursor() as cur:
        #         cur.execute(
        #             """
        #             SELECT
        #                 j.title,
        #                 c.name,
        #                 l.state,
        #                 l.country,
        #                 j.source_url,
        #                 i.name AS industry,
        #                 j.external_id
        #             FROM jobs j
        #             LEFT JOIN companies c ON c.id = j.company_id
        #             LEFT JOIN locations l ON l.id = j.location_id
        #             LEFT JOIN job_skills js ON js.job_id = j.id
        #             LEFT JOIN skills s ON s.id = js.skill_id
        #             LEFT JOIN industries i on i.id = j.industry_id
        #             WHERE
        #                 j.is_active = TRUE
        #                 AND (
        #                     j.title ILIKE %s OR
        #                     j.description_raw ILIKE %s OR
        #                     c.name ILIKE %s OR
        #                     s.name ILIKE %s
        #                 )
        #             GROUP BY j.id, c.name, l.state, l.country, i.name, j.external_id
        #             ORDER BY j.last_seen DESC
        #             LIMIT 10;
        #         """,
        #             (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"),
        #         )

        # jobs = "weather"

    return render_template(
        "find.html",
        q=q,
        weather=weather,
    )


@app.route("/")
def home():
    return render_template("index.html")


if __name__ == "__main__":
    if dev_mode:
        port = int(os.environ.get("PORT", 5500))  # dev port for livereload
        server = Server(app.wsgi_app)
        # watch templates and static CSS
        server.watch("templates/")
        server.watch("static/css/")
        # optional: watch Python files and reload server
        server.watch("*.py")
        server.serve(host="0.0.0.0", port=port, debug=True)
    else:
        port = int(os.environ.get("PORT", 8000))
        app.run(host="0.0.0.0", port=port, debug=True)
