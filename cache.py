#!/usr/bin/env python3
import datetime
from uuid import UUID

from flask import Flask, send_file, request, Response, render_template
import psycopg2
import psycopg2.extras
from psycopg2.errors import OperationalError
import requests
from werkzeug.exceptions import BadRequest, InternalServerError

import config


class CoverArtGenerator:
    """ Main engine for generating dynamic cover art. Given a design and data (e.g. stats) generate
        cover art from cover art images or text using the SVG format. """

    # Specify some operating limits
    MIN_IMAGE_SIZE = 128
    MAX_IMAGE_SIZE = 1024
    CAA_MISSING_IMAGE = "https://listenbrainz.org/static/img/cover-art-placeholder.jpg"

    # This grid tile designs (layouts?) are expressed as a dict with they key as dimension.
    # The value of the dict defines one design, with each cell being able to specify one or
    # more number of cells. Each string is a list of cells that will be used to define 
    # the bounding box of these cells. The cover art in question will be placed inside this
    # area.
    GRID_TILE_DESIGNS = {
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

    # Take time ranges and give correct english text
    time_range_to_english = { "week": "last week",
                              "month": "last month", 
                              "quarter": "last quarter", 
                              "half_yearly": "last 6 months",
                              "year": "last year",
                              "all_time": "of all time",
                              "this_week": "this week",
                              "this_month": "this month",
                              "this_year": "this year" }

    def __init__(self, dimension, image_size, background="#FFFFFF",
                 skip_missing=True, show_caa_image_for_missing_covers=True):
        self.dimension = dimension
        self.image_size = image_size
        self.background = background
        self.skip_missing = skip_missing
        self.show_caa_image_for_missing_covers = show_caa_image_for_missing_covers
        self.tile_size = image_size // dimension  # This will likely need more cafeful thought due to round off errors

    def parse_color_code(self, color_code):
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

    def validate_parameters(self):
        """ Validate the parameters for the cover art designs. """

        if self.dimension not in (2, 3, 4, 5):
            return "dimmension must be between 2 and 5, inclusive."

        bg_color = self.parse_color_code(self.background)
        if self.background not in ("transparent", "white", "black") and bg_color is None:
            return f"background must be one of transparent, white, black or a color code #rrggbb, not {self.background}"

        if self.image_size < CoverArtGenerator.MIN_IMAGE_SIZE or self.image_size > CoverArtGenerator.MAX_IMAGE_SIZE:
            return f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive."

        if not isinstance(self.skip_missing, bool):
            return f"option skip-missing must be of type boolean."

        return None


    def get_caa_id(self, release_mbid):
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

    def get_tile_position(self, tile):
        """ Calculate the position of a given tile, return (x1, y1, x2, y2). The math
            in this setup may seem a bit wonky, but that is to ensure that we don't have
            round-off errors that will manifest as line artifacts on the resultant covers"""

        if tile < 0 or tile >= self.dimension * self.dimension:
            return (None, None)

        x = tile % self.dimension
        y = tile // self.dimension

        x1 = int(x * self.tile_size)
        y1 = int(y * self.tile_size)
        x2 = int((x+1) * self.tile_size)
        y2 = int((y+1) * self.tile_size)

        if x == self.dimension - 1:
            x2 = self.image_size - 1
        if y == self.dimension - 1:
            y2 = self.image_size - 1

        return (x1, y1, x2, y2)

    def calculate_bounding_box(self, address):
        """ Given a cell 'address' return its bounding box. """

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
            x1, y1, x2, y2 = self.get_tile_position(tile)

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

    def resolve_cover_art(self, release_mbid):
        """ Translate a release_mbid into a cover art URL. Return None if unresolvable. """

        if release_mbid is None:
            return None

        caa_id = self.get_caa_id(release_mbid)
        if caa_id is None:
            return None

        return f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}_thumb500.jpg"

    def load_images(self, mbids, tile_addrs=None, layout=None):
        """ Given a list of MBIDs and optional tile addresses, resolve all the cover art design, all the
            cover art to be used and then return the list of images and locations where they should be
            placed. """

        if layout is not None:
            addrs = self.GRID_TILE_DESIGNS[self.dimension][layout]
        elif tile_addrs is None:
            addrs = self.GRID_TILE_DESIGNS[self.dimension][0]
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
                    color = None
                    if url is None:
                        if self.skip_missing:
                            continue
                        else:
                            if self.show_caa_image_for_missing_covers:
                                url = self.CAA_MISSING_IMAGE
                            else:
                                url = None

                    break
                except IndexError:
                    if self.show_caa_image_for_missing_covers:
                        url = self.CAA_MISSING_IMAGE
                    else:
                        url = None
                    break

            if url is not None:
                images.append({"x": x1, "y": y1, "width": x2 - x1, "height": y2 - y1, "url": url})

        return images


    def download_user_stats(self, entity, user_name, date_range):

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


    def create_grid_stats_cover(self, user_name, time_range, layout):
        releases, _ = self.download_user_stats("release", user_name, time_range)
        if len(releases) == 0:
            raise BadRequest(f"user {user_name} does not have any releases we can fetch. :(")

        release_mbids = [ r["release_mbid"] for r in releases ]  
        images = self.load_images(release_mbids, layout=layout)
        if images is None:
            raise InternalServerError("Failed to create composite image.")

        return images, releases

    def create_artist_stats_cover(self, user_name, time_range):

        artists, total_count = self.download_user_stats("artist", user_name, time_range)
        if len(artists) == 0:
            raise BadRequest(f"user {user_name} does not have any artists we can fetch. :(")

        # TODO: Remove VA from this list

        metadata = { "user_name": user_name,
                     "date": datetime.datetime.now().strftime("%Y-%m-%d"),
                     "time_range": self.time_range_to_english[time_range],
                     "num_artists": total_count }
        return artists, metadata

    def create_release_stats_cover(self, user_name, time_range):

        releases, total_count = self.download_user_stats("release", user_name, time_range)
        if len(releases) == 0:
            raise BadRequest(f"user {user_name} does not have any releases we can fetch. :(")
        release_mbids = [ r["release_mbid"] for r in releases ]  

        images = self.load_images(release_mbids)
        if images is None:
            raise InternalServerError("Failed to create composite image.")

        metadata = { "user_name": user_name,
                     "date": datetime.datetime.now().strftime("%Y-%m-%d"), 
                     "time_range": self.time_range_to_english[time_range],
                     "num_releases": total_count }

        return images, releases, metadata


app = Flask(__name__, template_folder="template", static_folder="static", static_url_path="/static")

@app.route("/")
def index():
    return render_template("index.html", width="750", height="750")


@app.route("/coverart/grid/", methods=["POST"])
def cover_art_grid_post():

    r = request.json

    if "tiles" in r:
        cac = CoverArtGenerator(r["dimension"], r["image_size"], r["background"], r["skip-missing"], r["show-caa"])
        tiles = r["tiles"]
    else:
        if "layout" in r:
            layout = r["layout"]
        else:
            layout = 0
        cac = CoverArtGenerator(r["dimension"], r["image_size"], r["background"], r["skip-missing"], r["show-caa"], r["layout"])
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

    images = cac.load_images(r["release_mbids"], tile_addrs=tiles)
    if images is None:
        raise InternalServerError("Failed to grid cover art SVG")

    return render_template("svg-templates/simple-grid.svg",
                           background=r["background"],
                           images=images,
                           width=r["image_size"],
                           height=r["image_size"]), 200, {'Content-Type': 'image/svg+xml'}


@app.route("/coverart/grid-stats/<user_name>/<time_range>/<int:dimension>/<int:layout>/<int:image_size>", methods=["GET"])
def cover_art_grid_stats(user_name, time_range, dimension, layout, image_size):

    cac = CoverArtGenerator(dimension, image_size)
    err = cac.validate_parameters()
    if err is not None:
        raise BadRequest(err)

    try:
        _ = cac.GRID_TILE_DESIGNS[dimension][layout]
    except IndexError:
        return f"layout {layout} is not available for dimension {dimension}."

    images, _ = cac.create_grid_stats_cover(user_name, time_range, layout)
    if images is None:
        raise InternalServerError("Failed to create composite image.")

    return render_template("svg-templates/simple-grid.svg",
                           background=cac.background,
                           images=images,
                           width=image_size,
                           height=image_size), 200, {'Content-Type': 'image/svg+xml'}


@app.route("/coverart/<custom_name>/<user_name>/<time_range>/<int:image_size>", methods=["GET"])
def cover_art_custom_stats(custom_name, user_name, time_range, image_size):

    cac = CoverArtGenerator(3, image_size)
    err = cac.validate_parameters()
    if err is not None:
        raise BadRequest(err)

    if custom_name in ("designer-top-5"):
        artists, metadata = cac.create_artist_stats_cover(user_name, time_range)
        return render_template(f"svg-templates/{custom_name}.svg", 
                               artists=artists,
                               width=image_size,
                               height=image_size,
                               metadata=metadata), 200, {'Content-Type': 'image/svg+xml'}

    if custom_name in ("lps-on-the-floor", "designer-top-10", "designer-top-10-alt"):
        images, releases, metadata = cac.create_release_stats_cover(user_name, time_range)
        return render_template(f"svg-templates/{custom_name}.svg", 
                               images=images,
                               releases=releases,
                               width=image_size,
                               height=image_size,
                               metadata=metadata), 200, {'Content-Type': 'image/svg+xml'}

    raise BadRequest(f"Unkown custom cover art type {custom_name}")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
