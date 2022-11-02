#!/usr/bin/env python3
import datetime

from flask import Flask, send_file, request, Response, render_template
import requests
from werkzeug.exceptions import BadRequest, InternalServerError

import config

app = Flask(__name__, template_folder="template", static_folder="static", static_url_path="/static")

time_ranges = ["month", "week", "quarter", "half_yearly", "year", "all_time", "this_week", "this_month", "this_year"]


@app.route("/", methods=["GET"])
def index_get():
    return render_template("index.html", time_ranges=time_ranges)


@app.route("/similar-users", methods=["GET"])
def similar_users():

    image_size = 750
    user_name = request.args.get("user_name", None)
    time_range = request.args.get("time_range", None)
    if user_name is None or time_range is None:
        render_template("similar-users.html", error=f"You must provide user_name and time_range arguments to this page.")

    SERVER_URL = f"https://api.listenbrainz.org/1/user/{user_name}/similar-users"
    r = requests.get(SERVER_URL)
    if r.status_code != 200:
        render_template("similar-users.html",
                        error=f"Could not fetch similar users for user {user_name}. ({r.status_code}, {r.text})")

    try:
        similar_users = r.json()["payload"]
    except KeyError:
        return render_template("similar-users.html", error=f"Could not fetch similar users for user {user_name}.")

    for i, _ in enumerate(similar_users):
        similar_users[i]["similarity"] = int(similar_users[i]["similarity"] * 100)

    return render_template("similar-users.html",
                           image_size=image_size,
                           user_name=user_name,
                           time_range=time_range,
                           similar_users=similar_users[:10])


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
