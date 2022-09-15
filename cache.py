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

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

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

    def create_grid(self, order, image_size, release_mbids):
        """ Create a simple cover art grid. order specifies the number of images per axis,
            image_file the destination image file and release_mbids the list
            of release_mbids to use in the grid. """

        if order not in (2, 3, 4, 5):
            raise ValueError("Order must be between 2 and 5, inclusive.")

        if image_size < self.MIN_IMAGE_SIZE or image_size > self.MAX_IMAGE_SIZE:
            raise ValueError(f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive.")

        if image_size % order != 0:
            raise ValueError(f"image size must be a multiple of image order.")

        tile_size = image_size // order

        composite = Image(height=image_size, width=image_size, background="#000000")
        row = 0
        col = 0
        for mbid in release_mbids:
            cover_art_file = self.fetch(mbid)
            if cover_art_file is None:
                raise ValueError(f"Cover art not found for release_mbid {mbid}")

            cover = Image(filename=cover_art_file)
            cover.resize(tile_size, tile_size)

            composite.composite(left=col*tile_size, top=row*tile_size, image=cover)

            col += 1
            if col == order:
                col = 0
                row += 1

        obj = io.BytesIO()
        composite.format = 'jpeg'
        composite.save(file=obj)
        obj.seek(0, 0)

        return obj


#cac = CoverArtCache("coverart")
#cac.create_grid(2, 750, "image.jpg",
#                ["76df3287-6cda-33eb-8e9a-044b5e15ffdd", "3a25785e-50ec-383c-89cf-fd512181449c",
#                 "9db51cd6-38f6-3b42-8ad5-559963d68f35", "b2a820cc-c0ad-4aa3-a2a7-ed42ead88017"])


app = Flask(__name__)

@app.route("/coverart/grid/<int:order>/<int:image_size>/", methods=["GET"])
def cover_art_grid(order, image_size):

    if order not in (2, 3, 4, 5):
        raise BadRequest("Order must be between 2 and 5, inclusive.")

    if image_size < CoverArtCache.MIN_IMAGE_SIZE or image_size > CoverArtCache.MAX_IMAGE_SIZE:
        raise BadRequest(f"image size must be between {self.MIN_IMAGE_SIZE} and {self.MAX_IMAGE_SIZE}, inclusive.")

    if image_size % order != 0:
        raise BadRequest(f"image size must be a multiple of image order.")

    mbids = request.args.get("release_mbids", "").split(",") 
    if len(mbids) != order*order:
        raise BadRequest(f"Incorrect number of release_mbids specifieid. For order {order} it should be {order*order}.")

    for mbid in mbids:
        try:
            UUID(mbid)
        except ValueError:
            raise BadRequest(f"Invalid release_mbid {mbid} specified.")

    cac = CoverArtCache("/cache")
    image = cac.create_grid(order, image_size, mbids) 
    if image is None:
        raise BadRequest("Was not able to load all specified images.")

    return Response(response=image, status=200, mimetype="image/jpeg")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)
