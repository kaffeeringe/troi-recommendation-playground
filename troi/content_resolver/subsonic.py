import datetime
import logging
import time
import libsonic
import musicbrainzngs

from libsonic.errors import DataNotFoundError
import peewee
from tqdm import tqdm

from troi.content_resolver.database import Database
from troi.content_resolver.model.database import db
from troi.content_resolver.model.recording import Recording, FileIdType
from troi.content_resolver.utils import bcolors

# MusicBrainz API Client initialisieren
musicbrainzngs.set_useragent("TroiNextcloudResolver", "1.0", "https://github.com/metabrainz/troi")

logger = logging.getLogger("troi_subsonic_scan")

APP_LOG_LEVEL_NUM = 19
logging.addLevelName(APP_LOG_LEVEL_NUM, "NOTICE")

def applog(message, *args, **kwargs):
    logger._log(APP_LOG_LEVEL_NUM, message, args, **kwargs)


class FixedConnection(libsonic.Connection):
    """
    Subclass libsonic.Connection to handle Nextcloud Music Subsonic endpoint pathing,
    scheme retention, and standard SSL/TLS port handling.
    """
    def __init__(self, baseUrl, username, password, port=443, **kwargs):
        from urllib.parse import urlparse

        if not baseUrl.startswith("http://") and not baseUrl.startswith("https://"):
            baseUrl = f"https://{baseUrl}"

        parsed = urlparse(baseUrl)
        host_only = parsed.netloc.split(":")[0]
        clean_path = parsed.path.strip("/")

        kwargs["legacyAuth"] = True

        super().__init__(
            baseUrl=f"https://{host_only}",
            username=username,
            password=password,
            port=port,
            **kwargs
        )

        if clean_path:
            self._serverPath = f"/{clean_path}/rest"
        else:
            self._serverPath = "/rest"

    def _getApiUrl(self, action):
        return f"https://{self._hostname}{self._serverPath}/{action}.view"

    def _doInfoReq(self, req):
        return super()._doInfoReq(req)


class SubsonicDatabase(Database):
    BATCH_SIZE = 500

    def __init__(self, index_dir, config, quiet=False):
        self.config = config
        Database.__init__(self, index_dir, quiet)
        self.quiet = quiet
        self.mb_cache = {}  # Cache für bereits abgefragte MBIDs

    def sync(self):
        self.total = 0
        self.matched = 0
        self.error = 0

        self.run_sync()

        logger.info("Checked %s albums:" % self.total)
        logger.info("  %5d albums matched" % self.matched)
        logger.info("  %5d recordings with errors" % self.error)

    def connect(self):
        if not self.config:
            logger.error("Missing credentials to connect to subsonic")
            return None

        logger.info("[ connect to subsonic ]")

        port = getattr(self.config, "SUBSONIC_PORT", 443) or 443

        return FixedConnection(
            baseUrl=self.config.SUBSONIC_HOST,
            username=self.config.SUBSONIC_USER,
            password=self.config.SUBSONIC_PASSWORD,
            port=port
        )

    def fetch_mbid_from_musicbrainz(self, artist, album_name):
        cache_key = f"{artist} - {album_name}".lower()
        if cache_key in self.mb_cache:
            return self.mb_cache[cache_key]

        try:
            time.sleep(0.5)  # Pause für Rate Limiting der MusicBrainz API
            result = musicbrainzngs.search_release_groups(artist=artist, release=album_name, limit=1)
            release_groups = result.get("release-group-list", [])
            if release_groups:
                rg_id = release_groups[0]["id"]
                # MBID des ersten konkreten Release aus der Gruppe holen
                rel_result = musicbrainzngs.search_releases(rgid=rg_id, limit=1)
                releases = rel_result.get("release-list", [])
                if releases:
                    mbid = releases[0]["id"]
                    artist_mbid = releases[0]["artist-credit"][0]["artist"]["id"]
                    self.mb_cache[cache_key] = (mbid, artist_mbid)
                    return mbid, artist_mbid
        except Exception:
            pass

        self.mb_cache[cache_key] = (None, None)
        return None, None

    def run_sync(self):
        conn = self.connect()
        if not conn:
            return

        logger.info("[ load albums ]")
        album_ids = set()
        albums = []
        offset = 0
        while True:
            results = conn.getAlbumList2(ltype="alphabeticalByArtist", size=self.BATCH_SIZE, offset=offset)
            albums.extend(results["albumList2"]["album"])
            album_ids.update([r["id"] for r in results["albumList2"]["album"]])

            album_count = len(results["albumList2"]["album"])
            offset += album_count
            if album_count < self.BATCH_SIZE:
                break

        logger.info("[ loaded %d albums ]" % len(album_ids))

        if not self.quiet:
            pbar = tqdm(total=len(album_ids))

        for album in albums:
            album_info = conn.getAlbum(id=album["id"])

            album_mbid = album_info.get("musicBrainzId", album.get("musicBrainzId"))
            artist_mbid = None

            # Fallback auf MusicBrainz API
            if not album_mbid:
                album_mbid, artist_mbid = self.fetch_mbid_from_musicbrainz(album.get("artist", ""), album.get("name", ""))

            if not album_mbid:
                if not self.quiet:
                    msg = "subsonic album '%s' by '%s' has no MBID" % (album["name"], album["artist"])
                    pbar.write(bcolors.FAIL + "FAIL " + bcolors.ENDC + msg)
                    applog("FAIL: " + msg)
                self.error += 1
                if not self.quiet:
                    pbar.update(1)
                continue

            for song in album_info["album"]["song"]:
                # Falls Song-MBID fehlt, nutzen wir die Album-MBID als Fallback für die Zuordnung
                song_mbid = song.get("musicBrainzId", album_mbid)

                self.add_subsonic({
                    "artist_name": song["artist"],
                    "release_name": song["album"],
                    "recording_name": song["title"],
                    "artist_mbid": artist_mbid or "00000000-0000-0000-0000-000000000000",
                    "release_mbid": album_mbid,
                    "recording_mbid": song_mbid,
                    "duration": song.get("duration", 0) * 1000,
                    "track_num": song.get("track", 1),
                    "disc_num": song.get("discNumber", 1),
                    "subsonic_id": song["id"],
                    "mtime": datetime.datetime.now()
                })

            if not self.quiet:
                msg = "album %-50s %-50s" % (album["name"][:49], album["artist"][:49])
                pbar.write(bcolors.OKGREEN + "OK   " + bcolors.ENDC + msg)
                applog(msg)

            self.matched += 1
            self.total += 1
            if not self.quiet:
                pbar.update(1)

    def add_subsonic(self, mdata):
        with db.atomic():
            try:
                recording = Recording.select().where(Recording.file_id == mdata['subsonic_id']).get()
                recording.artist_name = mdata["artist_name"]
                recording.release_name = mdata["release_name"]
                recording.recording_name = mdata["recording_name"]
                recording.artist_mbid = mdata["artist_mbid"]
                recording.release_mbid = mdata["release_mbid"]
                recording.recording_mbid = mdata["recording_mbid"]
                recording.mtime = mdata["mtime"]
                recording.track_num = mdata["track_num"]
                recording.disc_num = mdata["disc_num"]
                recording.save()
            except peewee.DoesNotExist:
                recording = Recording.create(
                    file_id=mdata["subsonic_id"],
                    file_id_type=FileIdType(FileIdType.SUBSONIC_ID),
                    artist_name=mdata["artist_name"],
                    release_name=mdata["release_name"],
                    recording_name=mdata["recording_name"],
                    artist_mbid=mdata["artist_mbid"],
                    release_mbid=mdata["release_mbid"],
                    recording_mbid=mdata["recording_mbid"],
                    mtime=mdata["mtime"],
                    duration=mdata["duration"],
                    track_num=mdata["track_num"],
                    disc_num=mdata["disc_num"]
                )
                recording.save()
