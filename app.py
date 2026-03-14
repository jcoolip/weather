import os
from itertools import count

import psycopg2
from dotenv import load_dotenv
from flask import Flask, render_template, request
from livereload import Server

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
    return {"app": "weather", "status": "awesome"}


@app.route("/")
def home():
    return render_template("index.html")


if __name__ == "__main__":
    if dev_mode:
        port = int(os.environ.get("PORT", 5501))  # dev port for livereload
        server = Server(app.wsgi_app)
        # watch templates and static CSS
        server.watch("templates/")
        server.watch("static/css/")
        # optional: watch Python files and reload server
        server.watch("*.py")
        server.serve(host="0.0.0.0", port=port, debug=True)
    else:
        port = int(os.environ.get("PORT", 8001))
        app.run(host="0.0.0.0", port=port, debug=True)
