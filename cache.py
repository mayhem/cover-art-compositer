#!/usr/bin/env python3

import io
import os
from time import sleep
from uuid import UUID

from flask import Flask, send_file, request, Response, render_template
import psycopg2
import psycopg2.extras
from psycopg2.errors import OperationalError
import requests
from wand.image import Image
from wand.drawing import Drawing
from werkzeug.exceptions import BadRequest, InternalServerError

import config

# Dimension 2:
# 0  1
# 2  3
#
# Dimension 3:
# 0  1  2
# 3  4  5
# 6  7  8
#
# Dimension 4:
# 0   1  2  3
# 4   5  6  7
# 8   9 10 11
# 12 13 14 15
#
# Dimension 5:
# 0   1  2  3  4
# 5   6  7  8  9
# 10 11 12 13 14
# 15 16 17 18 19
# 20 21 22 23 24


class CoverArtCache:

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
        self.tile_size = image_size // dimension # This will likely need more cafeful thought due to round off errors

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

        if self.image_size < CoverArtCache.MIN_IMAGE_SIZE or self.image_size > CoverArtCache.MAX_IMAGE_SIZE:
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

    def _cache_path(self, release_mbid):
        """ Given a release_mbid, create the file system path to where the cover art should be saved and 
            ensure that the directory for it exists. """

        path = os.path.join(self.cache_dir, release_mbid[0], release_mbid[0:1], release_mbid[0:2])
        try:
            os.makedirs(path)
        except FileExistsError:
            pass
        return os.path.join(path, release_mbid + ".jpg")

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

    def _download_file(self, url):
        """ Download a file given a URL and return that file as file-like object. """

        sleep_duration = 2
        while True:
            headers = {'User-Agent': 'ListenBrainz Cover Art Compositor ( rob@metabrainz.org )'}
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                total = 0
                obj = io.BytesIO()
                for chunk in r:
                    total += len(chunk)
                    obj.write(chunk)
                obj.seek(0, 0)
                print("Loaded %d bytes" % total)
                return obj, ""

            if r.status_code in [403, 404]:
                return None, f"Could not load resource: {r.status_code}."

            if r.status_code == 429:
                log("Exceeded rate limit. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return None, "Timeout loading image, due to 429"

                continue

            if r.status_code == 503:
                log("Service not available. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return None, "Timeout loading image, 503"
                continue

            return None, "Unhandled status code: %d" % r.status_code

    def _download_cover_art(self, release_mbid, cover_art_file):
        """ The cover art for the given release mbid does not exist, so download it,
            save a local copy of it. """

        caa_id = self._get_caa_id(release_mbid)
        if caa_id is None:
            return "Could not find caa_id"

        url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}_thumb500.jpg"
        print(url)
        image, err = self._download_file(url)
        if image is None:
            print("Case 1");
            return err

        with open(cover_art_file, 'wb') as f:
            f.write(image.read())

        print("Case 2");
        return None

    def fetch(self, release_mbid):
        """ Fetch the cover art for the given release_mbid and return a path to where the image
            is located on the local fs. This function will check the local cache for the image and
            if it does not exist, it will be fetched from the archive and chached locally. """

        cover_art_file = self._cache_path(release_mbid)
        if not os.path.exists(cover_art_file):
            err = self._download_cover_art(release_mbid, cover_art_file)
            print("Fetch", err)
            if err is not None:
                return None, err

        return cover_art_file, ""


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

    def load_or_create_missing_cover_art_tile(self):
        if self.missing_cover_art_tile is None:
            match self.missing_art:
                case "caa-image":
                    jpg_obj = self._download_file(self.CAA_MISSING_IMAGE)
                    self.missing_cover_art_tile = Image(file=jpg_obj)
                    self.missing_cover_art_tile.resize(self.tile_size, self.tile_size)
                case "background":
                    self.missing_cover_art_tile = Image(width=self.tile_size, height=self.tile_size)
                case "white":
                    self.missing_cover_art_tile = Image(width=self.tile_size, height=self.tile_size, background="white")
                case "black":
                    self.missing_cover_art_tile = Image(width=self.tile_size, height=self.tile_size, background="black")

        return self.missing_cover_art_tile


    def create_grid(self, mbids, tile_addrs=None):

        composite = Image(height=self.image_size, width=self.image_size, background=self.background)
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

        i = 0
        for x1, y1, x2, y2 in tiles:
            i += 1
            while True:
                try:
                    mbid = mbids.pop(0)
                except IndexError:
                    cover_art = self.load_or_create_missing_cover_art_tile()

                cover_art, err = self.fetch(mbid)
                if cover_art is None:
                    print(f"Could not fetch cover art for {mbid}: {err}")
                    if self.skip_missing:
                        print("Skip nmissing and try again")
                        continue

                    cover_art = self.load_or_create_missing_cover_art_tile()
                break

            # Check to see if we have a string with a filename or loaded/prepped image (for missing images)
            if isinstance(cover_art, str):
                cover = Image(filename=cover_art)
                cover.resize(x2 - x1, y2 - y1)
            else:
                cover = cover_art

            composite.composite(left=x1, top=y1, image=cover)

        obj = io.BytesIO()
        composite.format = 'jpeg'
        composite.save(file=obj)
        obj.seek(0, 0)

        return obj


app = Flask(__name__, template_folder="template")

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/coverart/grid/", methods=["POST"])
def cover_art_grid_post():

    r = request.json

    if "tiles" in r:
        cac = CoverArtCache(config.CACHE_DIR, r["dimension"], r["image_size"],
                            r["background"], r["skip-missing"], r["missing-art"])
        tiles = r["tiles"]
    else:
        if "layout" in r:
            layout = r["layout"]
        else:
            layout = 0
        cac = CoverArtCache(config.CACHE_DIR, r["dimension"], r["image_size"],
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

def download_user_stats(user_name, date_range):

    if date_range not in ['week', 'month', 'quarter', 'half_yearly', 'year', 'all_time', 'this_week', 'this_month', 'this_year']:
        raise BadRequest("Invalid date range given.")

    url = f"https://api.listenbrainz.org/1/stats/user/{user_name}/releases"
    r = requests.get(url, { "range": date_range, "count": 100 })
    if r.status_code != 200:
        raise BadRequest("Fetching stats for user {user_name} failed: %d" % r.status_code)

    data = r.json()
    mbids = []
    for d in data["payload"]["releases"]:
        if d["release_mbid"] is not None:
            mbids.append(d["release_mbid"])

    return mbids


@app.route("/coverart/grid-stats/<user_name>/<range>/<int:dimension>/<int:layout>/<int:image_size>", methods=["GET"])
def cover_art_grid_stats(user_name, range, dimension, layout, image_size):

    release_mbids = download_user_stats(user_name, range)
    if len(release_mbids) == 0:
        raise BadRequest(f"user {user_name} does not have any releases we can fetch. :(")

    cac = CoverArtCache(config.CACHE_DIR, dimension, image_size, "black", True, "black", layout)
    err = cac.validate_parameters()
    if err is not None:
        raise BadRequest(err)

    image = cac.create_grid(release_mbids)
    if image is None:
        raise InternalServerError("Failed to create composite image.")

    return Response(response=image, status=200, mimetype="image/jpeg")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
