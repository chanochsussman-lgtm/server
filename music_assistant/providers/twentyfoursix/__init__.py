"""24Six music provider for Music Assistant."""
from __future__ import annotations

import base64
import json as _json
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, AsyncGenerator

import aiohttp

from music_assistant_models.config_entries import ConfigEntry, ProviderConfig
from music_assistant_models.enums import (
    ContentType,
    ImageType,
    MediaType,
    ProviderFeature,
    StreamType,
)
from music_assistant_models.errors import LoginFailed, MediaNotFoundError
from music_assistant_models.media_items import (
    Album,
    Artist,
    AudioFormat,
    ItemMapping,
    MediaItemImage,
    ProviderMapping,
    SearchResults,
    Track,
)
from music_assistant_models.streamdetails import StreamDetails

from music_assistant.constants import CONF_PASSWORD, CONF_USERNAME
from music_assistant.models.music_provider import MusicProvider

if TYPE_CHECKING:
    from music_assistant_models.provider import ProviderManifest
    from music_assistant import MusicAssistant

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://24six.app"
API_BASE = f"{BASE_URL}/api"

# Confirmed from DevTools:
#   POST /app/content/{content_id}/begin
#   Headers: X-XSRF-TOKEN, X-Requested-With, Cookie (24six_session + XSRF-TOKEN)
#   Body (JSON ~71 bytes): {"device_id": "<uuid>", "content_type": "music"}
#   Response: { content_id, stream_id, content_type,
#               url: "https://stream.mux.com/<id>.m3u8?token=<jwt>" }
BEGIN_ENDPOINT = f"{BASE_URL}/app/content"  # + /{content_id}/begin

# Refresh Mux signed URL when < 5 min remain on the JWT
TOKEN_REFRESH_BUFFER = 300

SUPPORTED_FEATURES = (
    ProviderFeature.SEARCH,
    ProviderFeature.BROWSE,
    ProviderFeature.LIBRARY_ARTISTS,
    ProviderFeature.LIBRARY_ALBUMS,
    ProviderFeature.LIBRARY_TRACKS,
    ProviderFeature.ARTIST_ALBUMS,
    ProviderFeature.ARTIST_TOPTRACKS,
)


# ---------------------------------------------------------------------------
# Entry point called by Music Assistant
# ---------------------------------------------------------------------------

async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> "TwentyFourSixProvider":
    """Initialize provider(instance) with given configuration."""
    prov = TwentyFourSixProvider(mass, manifest, config)
    await prov.handle_async_init()
    return prov


async def get_config_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,
    action: str | None = None,
    values: dict[str, ConfigEntry] | None = None,
) -> tuple[ConfigEntry, ...]:
    """Return config entries for this provider."""
    return ()


# ---------------------------------------------------------------------------
# Provider implementation
# ---------------------------------------------------------------------------

class TwentyFourSixProvider(MusicProvider):
    """Music provider for 24Six Jewish music streaming service."""

    _session: aiohttp.ClientSession | None = None
    _device_id: str = ""
    # { content_id: (mux_url, expiry_epoch) }
    _stream_url_cache: dict[str, tuple[str, int]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def supported_features(self) -> tuple[ProviderFeature, ...]:
        return SUPPORTED_FEATURES

    async def handle_async_init(self) -> None:
        self._device_id = str(uuid.uuid4())
        self._stream_url_cache = {}
        await self._login()

    async def unload(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "X-Requested-With": "XMLHttpRequest",
                    "Origin": BASE_URL,
                }
            )
        return self._session

    def _xsrf_header(self, session: aiohttp.ClientSession) -> dict[str, str]:
        """Build X-XSRF-TOKEN header from the session cookie jar."""
        for cookie in session.cookie_jar:
            if cookie.key == "XSRF-TOKEN":
                return {"X-XSRF-TOKEN": urllib.parse.unquote(cookie.value)}
        return {}

    async def _login(self) -> None:
        """Login to 24Six: GET /login for CSRF cookie, then POST credentials."""
        username: str = self.config.get_value(CONF_USERNAME)
        password: str = self.config.get_value(CONF_PASSWORD)
        session = await self._get_session()

        # Step 1: GET login page to receive XSRF-TOKEN cookie
        try:
            async with session.get(
                f"{BASE_URL}/login",
                headers={"Accept": "text/html,application/xhtml+xml"},
            ) as resp:
                resp.raise_for_status()
        except aiohttp.ClientError as exc:
            raise LoginFailed(f"24Six: unable to reach login page: {exc}") from exc

        # Step 2: POST credentials
        xsrf = self._xsrf_header(session)
        if not xsrf:
            self.logger.warning("24Six: XSRF-TOKEN cookie not found after GET /login")

        try:
            async with session.post(
                f"{BASE_URL}/login",
                json={"email": username, "password": password},
                headers=xsrf,
                allow_redirects=True,
            ) as resp:
                if resp.status not in (200, 201, 204, 302):
                    raise LoginFailed(
                        f"24Six login failed — HTTP {resp.status}. "
                        "Check your username and password."
                    )
                self.logger.info("24Six: logged in as %s", username)
        except aiohttp.ClientError as exc:
            raise LoginFailed(f"24Six login request failed: {exc}") from exc

    async def _api_get(self, url: str, params: dict | None = None) -> dict:
        """Authenticated GET, auto-retry once on 401."""
        session = await self._get_session()
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 401:
                    self.logger.warning("24Six: 401 on GET %s — re-logging in", url)
                    await self._login()
                    async with session.get(url, params=params) as resp2:
                        resp2.raise_for_status()
                        return await resp2.json(content_type=None)
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            self.logger.error("24Six GET error %s: %s", url, exc)
            return {}

    async def _api_post(self, url: str, body: dict) -> dict:
        """Authenticated POST, auto-retry once on 401."""
        session = await self._get_session()
        xsrf = self._xsrf_header(session)
        try:
            async with session.post(url, json=body, headers=xsrf) as resp:
                if resp.status == 401:
                    self.logger.warning("24Six: 401 on POST %s — re-logging in", url)
                    await self._login()
                    xsrf = self._xsrf_header(session)
                    async with session.post(url, json=body, headers=xsrf) as resp2:
                        resp2.raise_for_status()
                        return await resp2.json(content_type=None)
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            self.logger.error("24Six POST error %s: %s", url, exc)
            return {}

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(
        self,
        search_query: str,
        media_types: list[MediaType] | None = None,
        limit: int = 20,
    ) -> SearchResults:
        """Search 24Six for artists, albums and tracks."""
        data = await self._api_get(
            f"{BASE_URL}/app/music/search",
            params={"q": search_query},
        )
        props = data.get("props", {})
        results = SearchResults()

        if not media_types or MediaType.ARTIST in media_types:
            for item in (props.get("artists") or {}).get("tiles", []):
                results.artists.append(self._parse_artist(item))

        if not media_types or MediaType.ALBUM in media_types:
            for item in (props.get("collections") or {}).get("tiles", []):
                results.albums.append(self._parse_album(item))

        if not media_types or MediaType.TRACK in media_types:
            for item in (props.get("content") or {}).get("tiles", []):
                results.tracks.append(self._parse_track(item))

        return results

    # ------------------------------------------------------------------
    # Artists
    # ------------------------------------------------------------------

    async def get_library_artists(self) -> AsyncGenerator[Artist, None]:
        data = await self._api_get(f"{API_BASE}/music/artists/favorites")
        for item in data.get("data", []):
            yield self._parse_artist(item)

    async def get_artist(self, prov_artist_id: str) -> Artist:
        data = await self._api_get(f"{BASE_URL}/app/music/artist/{prov_artist_id}")
        artist_data = data.get("props", {}).get("artist") or data
        if not artist_data:
            raise MediaNotFoundError(f"Artist {prov_artist_id} not found on 24Six")
        return self._parse_artist(artist_data)

    async def get_artist_albums(self, prov_artist_id: str) -> list[Album]:
        data = await self._api_get(f"{BASE_URL}/app/music/artist/{prov_artist_id}")
        tiles = data.get("props", {}).get("collections", {}).get("tiles", [])
        return [self._parse_album(c) for c in tiles]

    async def get_artist_toptracks(self, prov_artist_id: str) -> list[Track]:
        data = await self._api_get(f"{BASE_URL}/app/music/artist/{prov_artist_id}")
        tiles = data.get("props", {}).get("content", {}).get("tiles", [])
        return [self._parse_track(t) for t in tiles]

    # ------------------------------------------------------------------
    # Albums
    # ------------------------------------------------------------------

    async def get_library_albums(self) -> AsyncGenerator[Album, None]:
        data = await self._api_get(f"{API_BASE}/music/collections/library")
        for item in data.get("data", []):
            yield self._parse_album(item)

    async def get_album(self, prov_album_id: str) -> Album:
        data = await self._api_get(f"{BASE_URL}/app/music/collection/{prov_album_id}")
        album_data = data.get("props", {}).get("collection") or data
        if not album_data:
            raise MediaNotFoundError(f"Album {prov_album_id} not found on 24Six")
        return self._parse_album(album_data)

    async def get_album_tracks(self, prov_album_id: str) -> list[Track]:
        data = await self._api_get(f"{BASE_URL}/app/music/collection/{prov_album_id}")
        props = data.get("props", {})
        tiles = props.get("content", {}).get("tiles", []) or props.get("tracks", [])
        return [self._parse_track(t) for t in tiles]

    # ------------------------------------------------------------------
    # Tracks
    # ------------------------------------------------------------------

    async def get_library_tracks(self) -> AsyncGenerator[Track, None]:
        data = await self._api_get(f"{API_BASE}/music/content/favorites")
        for item in data.get("data", []):
            yield self._parse_track(item)

    async def get_track(self, prov_track_id: str) -> Track:
        data = await self._api_get(f"{BASE_URL}/app/music/content/{prov_track_id}")
        track_data = data.get("props", {}).get("content") or data
        if not track_data:
            raise MediaNotFoundError(f"Track {prov_track_id} not found on 24Six")
        return self._parse_track(track_data)

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    async def _begin_stream(self, content_id: str) -> str:
        """
        Call POST /app/content/{content_id}/begin to get a signed Mux HLS URL.

        Confirmed from browser DevTools:
          POST https://24six.app/app/content/{content_id}/begin
          X-XSRF-TOKEN: <decoded XSRF-TOKEN cookie value>
          X-Requested-With: XMLHttpRequest
          Content-Type: application/json
          Body: {"device_id": "<uuid>", "content_type": "music"}   (~71 bytes)

          Response:
          {
            "content_id": 385629,
            "stream_id":  "s14463c5ca9480",
            "content_type": "music",
            "url": "https://stream.mux.com/<playback_id>.m3u8?token=<jwt>"
          }

        The JWT 'exp' claim is parsed so we cache the URL and only call
        /begin again when the token is close to expiry.
        """
        # Serve from cache if still valid
        cached = self._stream_url_cache.get(content_id)
        if cached:
            mux_url, expiry = cached
            if time.time() < expiry - TOKEN_REFRESH_BUFFER:
                self.logger.debug("24Six: cached Mux URL for %s", content_id)
                return mux_url

        url = f"{BEGIN_ENDPOINT}/{content_id}/begin"
        body = {"device_id": self._device_id, "content_type": "music"}

        self.logger.debug("24Six: POST /begin for content_id=%s", content_id)
        data = await self._api_post(url, body)

        mux_url = data.get("url")
        if not mux_url:
            raise MediaNotFoundError(
                f"24Six: /begin returned no URL for content_id={content_id}. "
                f"Full response: {data}"
            )

        expiry = _parse_jwt_expiry(mux_url)
        self._stream_url_cache[content_id] = (mux_url, expiry)
        self.logger.debug(
            "24Six: Mux URL ready for content_id=%s (expires %s)", content_id, expiry
        )
        return mux_url

    async def get_stream_details(self, item_id: str) -> StreamDetails:
        """
        Return stream details.  Calls /begin to get the signed Mux HLS URL.
        MA passes this directly to ffmpeg which handles HLS segment fetching.
        """
        mux_url = await self._begin_stream(item_id)
        return StreamDetails(
            item_id=item_id,
            provider=self.instance_id,
            audio_format=AudioFormat(content_type=ContentType.HLS),
            stream_type=StreamType.HTTP,
            path=mux_url,
        )

    # ------------------------------------------------------------------
    # Data mapping helpers
    # ------------------------------------------------------------------

    def _img_url(self, raw_url: str | None) -> str | None:
        if not raw_url:
            return None
        return raw_url.split("?")[0]

    def _parse_artist(self, data: dict) -> Artist:
        artist_id = str(data.get("id", ""))
        artist = Artist(
            item_id=artist_id,
            provider=self.instance_id,
            name=data.get("name") or data.get("title") or "Unknown Artist",
            provider_mappings={
                ProviderMapping(
                    item_id=artist_id,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    url=data.get("preview_url"),
                )
            },
        )
        if data.get("bio"):
            artist.metadata.description = data["bio"]
        img = self._img_url(data.get("img"))
        if img:
            artist.metadata.images = [
                MediaItemImage(type=ImageType.THUMB, path=img, provider=self.instance_id)
            ]
        return artist

    def _parse_album(self, data: dict) -> Album:
        album_id = str(data.get("id", ""))
        album = Album(
            item_id=album_id,
            provider=self.instance_id,
            name=data.get("title") or "Unknown Album",
            artists=[
                ItemMapping(
                    item_id=str(a["id"]),
                    provider=self.instance_id,
                    name=a.get("name", ""),
                )
                for a in data.get("artists", [])
            ],
            provider_mappings={
                ProviderMapping(
                    item_id=album_id,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    url=data.get("preview_url"),
                )
            },
        )
        if data.get("release_date"):
            try:
                album.year = int(data["release_date"][:4])
            except (ValueError, IndexError):
                pass
        img = self._img_url(data.get("img"))
        if img:
            album.metadata.images = [
                MediaItemImage(type=ImageType.THUMB, path=img, provider=self.instance_id)
            ]
        return album

    def _parse_track(self, data: dict) -> Track:
        track_id = str(data.get("id", ""))
        collection = data.get("collection") or {}
        track = Track(
            item_id=track_id,
            provider=self.instance_id,
            name=data.get("title") or "Unknown Track",
            artists=[
                ItemMapping(
                    item_id=str(a["id"]),
                    provider=self.instance_id,
                    name=a.get("name", ""),
                )
                for a in data.get("artists", [])
            ],
            album=(
                ItemMapping(
                    item_id=str(collection["id"]),
                    provider=self.instance_id,
                    name=collection.get("title", ""),
                )
                if collection.get("id")
                else None
            ),
            duration=data.get("length", 0),
            track_number=data.get("track_num"),
            provider_mappings={
                ProviderMapping(
                    item_id=track_id,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    audio_format=AudioFormat(content_type=ContentType.HLS),
                    url=data.get("preview_url"),
                )
            },
        )
        img = self._img_url(data.get("img"))
        if img:
            track.metadata.images = [
                MediaItemImage(type=ImageType.THUMB, path=img, provider=self.instance_id)
            ]
        return track


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _parse_jwt_expiry(mux_url: str) -> int:
    """
    Decode the 'exp' claim from the JWT token in a Mux signed URL.
    Returns epoch int, or now+3600 on any parse failure.
    """
    try:
        qs = urllib.parse.urlparse(mux_url).query
        token = urllib.parse.parse_qs(qs).get("token", [None])[0]
        if not token:
            return int(time.time()) + 3600
        payload_b64 = token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = _json.loads(base64.urlsafe_b64decode(payload_b64))
        return int(payload.get("exp", time.time() + 3600))
    except Exception:  # noqa: BLE001
        return int(time.time()) + 3600
