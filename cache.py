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
from werkzeug.exceptions import BadRequest

import config


class CoverArtCache:

    MIN_IMAGE_SIZE = 128
    MAX_IMAGE_SIZE = 1024

    def __init__(self, cache_dir, dimension, image_size, background="#000000"):
        self.cache_dir = cache_dir
        self.dimension = dimension
        self.image_size = image_size
        self.background = background
        self.tile_size = image_size // dimension # This will likely need more cafeful thought due to round off errors

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

    def _download_cover_art(self, release_mbid, cover_art_file):
        """ The cover art for the given release mbid does not exist, so download it,
            save a local copy of it. """

        caa_id = self._get_caa_id(release_mbid)
        if caa_id is None:
            return False

        sleep_duration = 2
        while True:
            headers = {'User-Agent': 'ListenBrainz Cover Art Compositor ( rob@metabrainz.org )'}
            url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}_thumb500.jpg"
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                with open(cover_art_file, 'wb') as f:
                    for chunk in r:
                        f.write(chunk)
                return True

            if r.status_code in [403, 404]:
                return False

            if r.status_code == 429:
                log("Exceeded rate limit. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return False

                continue

            if r.status_code == 503:
                log("Service not available. sleeping %d seconds." % sleep_duration)
                sleep(sleep_duration)
                sleep_duration *= 2
                if sleep_duration > 100:
                    return False
                continue

            log("Unhandled %d" % r.status_code)
            return False

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
            if tile < 0 or tile >= self.dimension:
                return (None, None, None, None)

        x1 = y1 = x2 = y2 = -1
        for tile in tiles:
            x, y = self.get_tile_position(tile)
            if x1 < 0 or x < x1:
                x1 = x
            if y1 < 0 or y < y1:
                y1 = y
            if x2 < 0 or x > x2:
                x2 = x
            if y2 < 0 or y > y2:
                y2 = y

        return (x1, y1, x2, y2)



    def get_tile_postion(self, tile):
        """ Calculate the position of a given tile, return (x, y) """

        if tile < 0 or tile >= self.dimenson * self.dimension:
            return (None, None)

        return (int(tile % dimension * tile_size), int(tile // dimension * tile_size))


    def create_grid(self, tiles):
        composite = Image(height=self.image_size, width=self.image_size, background=self.background)
        for x1, y1, x2, y3, mbid in tiles:
            cover_art_file = self.fetch(mbid)
            if cover_art_file is None:
                raise ValueError(f"Cover art not found for release_mbid {mbid}")

            cover = Image(filename=cover_art_file)
            cover.resize(x2 - x1, y2 - y1)
            composite.composite(left=x1, top=y1, image=cover)

        obj = io.BytesIO()
        composite.format = 'jpeg'
        composite.save(file=obj)
        obj.seek(0, 0)

        return obj


app = Flask(__name__)

@app.route("/coverart/grid/<int:dimension>/<int:image_size>/", methods=["GET"])
def cover_art_grid_get(dimension, image_size):

    if dimension not in (2, 3, 4, 5):
        raise BadRequest("dimension must be between 2 and 5, inclusive.")

    if image_size < CoverArtCache.MIN_IMAGE_SIZE or image_size > CoverArtCache.MAX_IMAGE_SIZE:
        raise BadRequest(f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive.")

    mbids = request.args.get("release_mbids", "").split(",") 
    if len(mbids) != dimension*dimension:
        raise BadRequest(f"Incorrect number of release_mbids specifieid. For dimension {dimension} it should be {dimension*dimension}.")

    for mbid in mbids:
        try:
            UUID(mbid)
        except ValueError:
            raise BadRequest(f"Invalid release_mbid {mbid} specified.")

    tiles = []
    for addr, mbid in enumerate(mbids):
        x1, y1, x2, y2 = self.calculate_bounding_box(addr):
        if x1 is None:
            raise BadRequest(f"Invalid address {addr} specified.")
        tiles.append((x1, y1, x2, y2, mbid))

    cac = CoverArtCache("/cache", dimension, image_size)
    image = cac.create_grid(tiles)
    if image is None:
        raise BadRequest("Was not able to load all specified images.")

    return Response(response=image, status=200, mimetype="image/jpeg")


@app.route("/coverart/grid/", methods=["POST"])
def cover_art_grid_post():

    r = request.json()
    dimension = r["dimension"]
    image_size = r["image_size"]
    background = r["background"]
    mbids = r["tiles"]

    if dimension not in (2, 3, 4, 5):
        raise BadRequest("dimmension must be between 2 and 5, inclusive.")

    if image_size < CoverArtCache.MIN_IMAGE_SIZE or image_size > CoverArtCache.MAX_IMAGE_SIZE:
        raise BadRequest(f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive.")

    # Check to see if we are making a simple grid or a complex one
    if type(mbids[0]) == str:
        for mbid in mbids:
            try:
                UUID(mbid)
            except ValueError:
                raise BadRequest(f"Invalid release_mbid {mbid} specified.")

        image = self.create_simple_grid(mbids)
        if image is None:
            raise BadRequest("Was not able to load all specified images.")

    if type(mbids[0]) == list:
        raise BadRequest("tiles must be a list of lists or a list of release_mbids")

    # Yes, this is complex grid!
    tiles = []
    for addr, mbid in mbids:
        x1, y1, x2, y2 = self.calculate_bounding_box(addr):
        if x1 is None:
            raise BadRequest(f"Invalid address {addr} specified.")
        tiles.append((x1, y1, x2, y2, mbid))
            
    cac = CoverArtCache("/cache", dimension, image_size, background)
    image = cac.create_grid(tiles)
    if image is None:
        raise BadRequest("Was not able to load all specified images.")

    return Response(response=image, status=200, mimetype="image/jpeg")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)
