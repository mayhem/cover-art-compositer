#!/usr/bin/env python3

import io
import os
from time import sleep
from uuid import UUID

from flask import Flask, send_file, request, Response
import psycopg2
import psycopg2.extras
from psycopg2.errors import OperationalError
import requests
from wand.image import Image
from wand.drawing import Drawing
from werkzeug.exceptions import BadRequest, InternalServerError

import config


class CoverArtCache:

    MIN_IMAGE_SIZE = 128
    MAX_IMAGE_SIZE = 1024
    CAA_MISSING_IMAGE = "https://listenbrainz.org/static/img/cover-art-placeholder.jpg"

    def __init__(self, cache_dir, dimension, image_size, background="#000000", skip_missing=True, missing_art="caa-image"):
        self.cache_dir = cache_dir
        self.dimension = dimension
        self.image_size = image_size
        self.background = background
        self.skip_missing = skip_missing
        self.missing_art = missing_art
        self.missing_cover_art_tile = None

        bg_color = self._parse_color_code(background)
        if background not in ("transparent", "white", "black") and bg_color is None:
            raise BadRequest(f"background must be one of transparent, white, black or a color code #rrggbb, not {background}")

        if dimension not in (2, 3, 4, 5):
            raise BadRequest("dimmension must be between 2 and 5, inclusive.")

        if image_size < CoverArtCache.MIN_IMAGE_SIZE or image_size > CoverArtCache.MAX_IMAGE_SIZE:
            raise BadRequest(f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive.")

        if not isinstance(self.skip_missing, bool):
            raise BadRequest(f"option skip-missing must be of type boolean.")

        if missing_art not in ("caa-image", "background", "white", "black"):
            raise BadRequest("missing-art option must be one of caa-image, background, white or black.")

        self.tile_size = image_size // dimension # This will likely need more cafeful thought due to round off errors

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
                obj = io.BytesIO()
                for chunk in r:
                    obj.write(chunk)
                obj.seek(0, 0)
                return obj

            if r.status_code in [403, 404]:
                return None

            if r.status_code == 429:
                log("Exceeded rate limit. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return None

                continue

            if r.status_code == 503:
                log("Service not available. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return None
                continue

            log("Unhandled %d" % r.status_code)
            return None

    def _download_cover_art(self, release_mbid, cover_art_file):
        """ The cover art for the given release mbid does not exist, so download it,
            save a local copy of it. """

        caa_id = self._get_caa_id(release_mbid)
        if caa_id is None:
            return False

        url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}_thumb500.jpg"
        image = self._download_file(url)
        if image is None:
            return False

        with open(cover_art_file, 'wb') as f:
            f.write(image.read())

        return True

    def fetch(self, release_mbid):
        """ Fetch the cover art for the given release_mbid and return a path to where the image
            is located on the local fs. This function will check the local cache for the image and
            if it does not exist, it will be fetched from the archive and chached locally. """

        cover_art_file = self._cache_path(release_mbid)
        if not os.path.exists(cover_art_file):
            if not self._download_cover_art(release_mbid, cover_art_file):
                return None

        return cover_art_file


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
                    self.missing_cover_art_tile = Image(width=self.tile_size, height=self.tile_size, bg="white")
                case "black":
                    self.missing_cover_art_tile = Image(width=self.tile_size, height=self.tile_size, bg="black")

        return self.missing_cover_art_tile


    def create_grid(self, mbids, tiles, addrs):
        composite = Image(height=self.image_size, width=self.image_size, background=self.background)
        i = 0
        for x1, y1, x2, y2 in tiles:
            i += 1
            while True:
                try:
                    mbid = mbids.pop(0)
                except IndexError:
                    cover_art = self.load_or_create_missing_cover_art_tile()

                cover_art = self.fetch(mbid)
                if cover_art is None:
                    print(f"Cound not fetch cover art for {mbid}")
                    if self.skip_missing:
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


app = Flask(__name__)

@app.route("/coverart/grid/", methods=["POST"])
def cover_art_grid_post():

    r = request.json
    cac = CoverArtCache(config.CACHE_DIR, r["dimension"], r["image_size"],
                        r["background"], r["skip-missing"], r["missing-art"])

    if not isinstance(r["release_mbids"], list):
        raise BadRequest("release_mbids must be a list of strings specifying release_mbids")

    for mbid in r["release_mbids"]:
        try:
            UUID(mbid)
        except ValueError:
            raise BadRequest(f"Invalid release_mbid {mbid} specified.")

    if "tiles" not in r:
        addrs = ["%d" % i for i in range(self.dimension * self.dimension)]
    elif not isinstance(r["tiles"], list):
        raise BadRequest(f"tiles must specify a list of tile addresses.")
    else:
        addrs = r["tiles"]

    tiles = []
    for addr in addrs:
        x1, y1, x2, y2 = cac.calculate_bounding_box(addr)
        if x1 is None:
            raise BadRequest(f"Invalid address {addr} specified.")
        tiles.append((x1, y1, x2, y2))

    image = cac.create_grid(r["release_mbids"], tiles, addrs)
    if image is None:
        raise InternalServerError("Failed to create composite image.")

    return Response(response=image, status=200, mimetype="image/jpeg")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
