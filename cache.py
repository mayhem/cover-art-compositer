#!/usr/bin/env python3
import datetime
import io
import os
from time import sleep
from uuid import UUID

from flask import Flask, send_file, request, Response, render_template
import psycopg2
import psycopg2.extras
from psycopg2.errors import OperationalError
import requests
from werkzeug.exceptions import BadRequest, InternalServerError

import config


class CoverArtCompositor:

    MIN_IMAGE_SIZE = 128
    MAX_IMAGE_SIZE = 1024
    CAA_MISSING_IMAGE = "https://listenbrainz.org/static/img/cover-art-placeholder.jpg"

    TILE_DESIGNS = {
        2: [
            ["0", "1", "2", "3"],
        ],
        3: [
            ["0", "1", "2", "3", "4", "5", "6", "7", "8"],
            ["0,1,3,4", "2", "5", "6", "7", "8"],
            ["0", "1", "2", "3", "4,5,7,8", "6"],
        ],
        4: [
            ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"],
            ["5,6,9,10", "0", "1", "2", "3", "4", "7", "8", "11", "12", "13", "14", "15"],
            ["0,1,4,5", "10,11,14,15", "2", "3", "6", "7", "8", "9", "12", "13"],
            ["0,1,2,4,5,6,8,9,10", "3", "7", "11", "12", "13", "14", "15"],
        ],
        5: [
            ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
             "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24"],
            ["0,1,2,5,6,7,10,11,12", "3,4,8,9", "15,16,20,21", "13", "14", "17", "18", "19", "22", "23", "24"]
        ]
    }

    def __init__(self, cache_dir, dimension, image_size, background="#000000",
                 skip_missing=True, missing_art="caa-image", layout=None):
        self.cache_dir = cache_dir
        self.dimension = dimension
        self.image_size = image_size
        self.background = background
        self.skip_missing = skip_missing
        self.missing_art = missing_art
        self.missing_cover_art_tile = None
        self.layout = layout
        self.tile_size = image_size // dimension  # This will likely need more cafeful thought due to round off errors

    def validate_parameters(self):
        """ Validate the parameters for the cover art designs. """

        if self.dimension not in (2, 3, 4, 5):
            return "dimmension must be between 2 and 5, inclusive."

        if self.layout is not None:
            try:
                _ = self.TILE_DESIGNS[self.dimension][self.layout]
            except IndexError:
                return f"layout {self.layout} is not available for dimension {self.dimension}."

        bg_color = self._parse_color_code(self.background)
        if self.background not in ("transparent", "white", "black") and bg_color is None:
            return f"background must be one of transparent, white, black or a color code #rrggbb, not {self.background}"

        if self.image_size < CoverArtCompositor.MIN_IMAGE_SIZE or self.image_size > CoverArtCompositor.MAX_IMAGE_SIZE:
            return f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive."

        if not isinstance(self.skip_missing, bool):
            return f"option skip-missing must be of type boolean."

        if self.missing_art not in ("caa-image", "background", "white", "black"):
            return "missing-art option must be one of caa-image, background, white or black."

        return None

    def _parse_color_code(self, color_code):
        if not color_code.startswith("#"):
            return None

        try:
            r = int(color_code[1:3], 16)
        except ValueError:
            return None

        try:
            g = int(color_code[3:5], 16)
        except ValueError:
            return None

        try:
            b = int(color_code[5:7], 16)
        except ValueError:
            return None

        return (r, g, b)

    def _get_caa_id(self, release_mbid):
        """ Fetch the CAA id for the front image for the given release_mbid """

        query = """SELECT caa.id AS caa_id
                     FROM cover_art_archive.cover_art caa
                     JOIN cover_art_archive.cover_art_type cat
                       ON cat.id = caa.id
                     JOIN musicbrainz.release
                       ON caa.release = release.id
                    WHERE type_id = 1
                      AND release.gid = %s"""

        with psycopg2.connect(config.MBID_MAPPING_DATABASE_URI) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute(query, (release_mbid,))
                row = curs.fetchone()
                if row:
                    return row["caa_id"]
                else:
                    return None

    def calculate_bounding_box(self, address):
        tiles = address.split(",")
        try:
            for i in range(len(tiles)):
                tiles[i] = int(tiles[i].strip())
        except ValueError:
            return (None, None, None, None)

        for tile in tiles:
            if tile < 0 or tile >= (self.dimension*self.dimension):
                return (None, None, None, None)

        for i, tile in enumerate(tiles):
            x1, y1 = self.get_tile_position(tile)
            x2 = x1 + self.tile_size
            y2 = y1 + self.tile_size

            if i == 0:
                bb_x1 = x1
                bb_y1 = y1
                bb_x2 = x2
                bb_y2 = y2
                continue

            bb_x1 = min(bb_x1, x1)
            bb_y1 = min(bb_y1, y1)
            bb_x1 = min(bb_x1, x2)
            bb_y1 = min(bb_y1, y2)
            bb_x2 = max(bb_x2, x1)
            bb_y2 = max(bb_y2, y1)
            bb_x2 = max(bb_x2, x2)
            bb_y2 = max(bb_y2, y2)

        return (bb_x1, bb_y1, bb_x2, bb_y2)

    def get_tile_position(self, tile):
        """ Calculate the position of a given tile, return (x, y) """

        if tile < 0 or tile >= self.dimension * self.dimension:
            return (None, None)

        return (int(tile % self.dimension * self.tile_size), int(tile // self.dimension * self.tile_size))

    def resolve_cover_art(self, release_mbid):
        if release_mbid is None:
            return None

        caa_id = self._get_caa_id(release_mbid)
        if caa_id is None:
            return None

        return f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}_thumb500.jpg"

    def create_grid(self, mbids, tile_addrs=None):

        if self.layout is not None:
            addrs = self.TILE_DESIGNS[self.dimension][self.layout]
        elif tile_addrs is None:
            addrs = self.TILE_DESIGNS[self.dimension][0]
        else:
            addrs = tile_addrs

        tiles = []
        for addr in addrs:
            x1, y1, x2, y2 = self.calculate_bounding_box(addr)
            if x1 is None:
                raise BadRequest(f"Invalid address {addr} specified.")
            tiles.append((x1, y1, x2, y2))

        images = []
        for x1, y1, x2, y2 in tiles:
            while True:
                try:
                    url = self.resolve_cover_art(mbids.pop(0))
                    if url is None:
                        if self.skip_missing:
                            continue
                        else:
                            url = self.CAA_MISSING_IMAGE
                    break
                except IndexError:
                    url = self.CAA_MISSING_IMAGE
                    break

            images.append({"x": x1, "y": y1, "width": x2 - x1, "height": y2 - y1, "url": url})

        return images


app = Flask(__name__, template_folder="template", static_folder="static", static_url_path="/static")


@app.route("/")
def index():
    return render_template("index.html", width="750", height="750")


@app.route("/coverart/grid/", methods=["POST"])
def cover_art_grid_post():

    r = request.json

    if "tiles" in r:
        cac = CoverArtCompositor(config.CACHE_DIR, r["dimension"], r["image_size"],
                                 r["background"], r["skip-missing"], r["missing-art"])
        tiles = r["tiles"]
    else:
        if "layout" in r:
            layout = r["layout"]
        else:
            layout = 0
        cac = CoverArtCompositor(config.CACHE_DIR, r["dimension"], r["image_size"],
                                 r["background"], r["skip-missing"], r["missing-art"], r["layout"])
        tiles = None

    err = cac.validate_parameters()
    if err is not None:
        raise BadRequest(err)

    if not isinstance(r["release_mbids"], list):
        raise BadRequest("release_mbids must be a list of strings specifying release_mbids")

    for mbid in r["release_mbids"]:
        try:
            UUID(mbid)
        except ValueError:
            raise BadRequest(f"Invalid release_mbid {mbid} specified.")

    image = cac.create_grid(r["release_mbids"], tiles)
    if image is None:
        raise InternalServerError("Failed to create composite image.")

    return Response(response=image, status=200, mimetype="image/jpeg")


def download_user_stats(entity, user_name, date_range):

    if date_range not in ['week', 'month', 'quarter', 'half_yearly', 'year', 'all_time', 'this_week', 'this_month', 'this_year']:
        raise BadRequest("Invalid date range given.")

    if entity not in ("artist", "release", "recording"):
        raise BadRequest("Stats entity must be one of artist, release or recording.")

    url = f"https://api.listenbrainz.org/1/stats/user/{user_name}/{entity}s"
    r = requests.get(url, {"range": date_range, "count": 100})
    if r.status_code != 200:
        raise BadRequest("Fetching stats for user {user_name} failed: %d" % r.status_code)

    data = r.json()["payload"]
    return data[entity + "s"], data[f"total_{entity}_count"]


@app.route("/coverart/grid-stats/<user_name>/<time_range>/<int:dimension>/<int:layout>/<int:image_size>", methods=["GET"])
def cover_art_grid_stats(user_name, time_range, dimension, layout, image_size):

    releases, _ = download_user_stats("release", user_name, time_range)
    if len(releases) == 0:
        raise BadRequest(f"user {user_name} does not have any releases we can fetch. :(")

    release_mbids = [ r["release_mbid"] for r in releases ]  

    cac = CoverArtCompositor(config.CACHE_DIR, dimension, image_size, "black", True, "black", layout)
    err = cac.validate_parameters()
    if err is not None:
        raise BadRequest(err)

    images = cac.create_grid(release_mbids)
    if images is None:
        raise InternalServerError("Failed to create composite image.")

    return render_template("svg-templates/simple-grid.svg", images=images, width=image_size, height=image_size), \
        200, {'Content-Type': 'image/svg+xml'}


@app.route("/coverart/<custom_name>/<user_name>/<time_range>/<int:image_size>", methods=["GET"])
def cover_art_custom_stats(custom_name, user_name, time_range, image_size):

    if custom_name not in ("cover-art-on-floor", "designer"):
        raise BadRequest(f"Unkown custom cover art type {custom_name}")

    if custom_name in ("designer"):
        return custom_artist_cover_art(custom_name, user_name, time_range, image_size)

    if custom_name in ("cover-art-on-floor"):
        return custom_release_cover_art(custom_name, user_name, time_range, image_size)

def custom_release_cover_art(custom_name, user_name, time_range, image_size):
    releases, total_count = download_user_stats("release", user_name, time_range)
    if len(releases) == 0:
        raise BadRequest(f"user {user_name} does not have any releases we can fetch. :(")
    release_mbids = [ r["release_mbid"] for r in releases ]  

    cac = CoverArtCompositor(config.CACHE_DIR, 3, image_size, "black", True, "black")
    err = cac.validate_parameters()
    if err is not None:
        raise BadRequest(err)

    images = cac.create_grid(release_mbids)
    if images is None:
        raise InternalServerError("Failed to create composite image.")

    metadata = { "user_name": user_name,
                 "date": datetime.datetime.now().strftime("%Y-%m-%d"), 
                 "time_range": time_range,
                 "num_releases": total_count }
    return render_template(f"svg-templates/{custom_name}.svg", 
                           images=images,
                           releases=releases,
                           width=image_size,
                           height=image_size,
                           metadata=metadata), 200, {'Content-Type': 'image/svg+xml'}

def custom_artist_cover_art(custom_name, user_name, time_range, image_size):
    artists, total_count = download_user_stats("artist", user_name, time_range)
    if len(artists) == 0:
        raise BadRequest(f"user {user_name} does not have any artists we can fetch. :(")

    metadata = { "user_name": user_name,
                 "date": datetime.datetime.now().strftime("%Y-%m-%d"),
                 "time_range": time_range,
                 "num_artists": total_count }
    return render_template(f"svg-templates/{custom_name}.svg", 
                           artists=artists,
                           width=image_size,
                           height=image_size,
                           metadata=metadata), 200, {'Content-Type': 'image/svg+xml'}

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
