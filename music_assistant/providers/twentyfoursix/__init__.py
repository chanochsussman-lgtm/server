"""24Six music provider for Music Assistant."""
from __future__ import annotations

import base64
import json as _json
import time
import urllib.parse
import uuid
from typing import TYPE_CHECKING, AsyncGenerator

import aiohttp

from music_assistant_models.config_entries import ConfigEntry, ConfigValueType, ProviderConfig
from music_assistant_models.enums import (
    ConfigEntryType,
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
    BrowseFolder,
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
BEGIN_ENDPOINT = f"{BASE_URL}/app/content"  # + /{content_id}/begin
TOKEN_REFRESH_BUFFER = 300

SUPPORTED_FEATURES = {
    ProviderFeature.SEARCH,
    ProviderFeature.BROWSE,
    ProviderFeature.ARTIST_ALBUMS,
    ProviderFeature.ARTIST_TOPTRACKS,
}


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
    values: dict[str, ConfigValueType] | None = None,
) -> tuple[ConfigEntry, ...]:
    """Return config entries for this provider."""
    return (
        ConfigEntry(
            key=CONF_USERNAME,
            type=ConfigEntryType.STRING,
            label="Email / Username",
            required=True,
            description="Your 24Six account email address",
        ),
        ConfigEntry(
            key=CONF_PASSWORD,
            type=ConfigEntryType.SECURE_STRING,
            label="Password",
            required=True,
            description="Your 24Six account password",
        ),
    )


# ---------------------------------------------------------------------------
# Provider implementation
# ---------------------------------------------------------------------------

class TwentyFourSixProvider(MusicProvider):
    """Music provider for 24Six Jewish music streaming service."""

    _session: aiohttp.ClientSession | None = None
    _device_id: str = ""
    _stream_url_cache: dict[str, tuple[str, int]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def supported_features(self) -> set[ProviderFeature]:
        return SUPPORTED_FEATURES

    async def handle_async_init(self) -> None:
        self._device_id = str(uuid.uuid4())
        self._stream_url_cache = {}
        await self._login()
        await self._select_profile()

    async def unload(self, *args, **kwargs) -> None:
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
        for cookie in session.cookie_jar:
            if cookie.key == "XSRF-TOKEN":
                return {"X-XSRF-TOKEN": urllib.parse.unquote(cookie.value)}
        return {}

    async def _login(self) -> None:
        """Login to 24Six matching the full browser flow:
        1. GET /login → CSRF token
        2. POST /check-existing-user with _token+email+password → profiles list
        3. POST /profiles/pin-check with profile_id → pre-selects profile
        4. POST /login with credentials → finalize auth
        """
        username: str = self.config.get_value(CONF_USERNAME)
        password: str = self.config.get_value(CONF_PASSWORD)
        session = await self._get_session()

        # Step 1: GET /login to obtain XSRF-TOKEN cookie and extract _token from HTML
        import re
        _token = ""
        try:
            async with session.get(
                f"{BASE_URL}/login",
                headers={"Accept": "text/html,application/xhtml+xml"},
            ) as resp:
                resp.raise_for_status()
                html = await resp.text()
                m = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
                if not m:
                    m = re.search(r'name="_token"[^>]+value="([^"]+)"', html)
                if m:
                    _token = m.group(1)
                    self.logger.info("24Six: extracted _token length=%s", len(_token))
                else:
                    self.logger.warning("24Six: could not find _token in login HTML")
        except aiohttp.ClientError as exc:
            raise LoginFailed(f"24Six: unable to reach login page: {exc}") from exc

        xsrf = self._xsrf_header(session)

        # Step 2: POST /check-existing-user with form data to get profiles list
        profile_id = None
        try:
            async with session.post(
                f"{BASE_URL}/check-existing-user",
                data={"_token": _token, "email": username, "password": password},
                headers=xsrf,
            ) as resp:
                data = await resp.json(content_type=None)
                self.logger.info("24Six: check-existing-user status=%s keys=%s", resp.status, list(data.keys()) if isinstance(data, dict) else type(data).__name__)
                # Response is {"user": {..., "profiles": [...]}} or similar
                if isinstance(data, list):
                    profiles = data
                elif isinstance(data, dict):
                    user = data.get("user") or {}
                    profiles = (
                        data.get("profiles")
                        or user.get("profiles")
                        or data.get("data")
                        or []
                    )
                    self.logger.info("24Six: user keys=%s profiles_count=%s", list(user.keys())[:10], len(profiles))
                else:
                    profiles = []
                for p in (profiles or []):
                    if "chanoch" in (p.get("name") or "").strip().lower():
                        profile_id = p.get("permission_id") or p.get("id")
                        break
                if not profile_id and profiles:
                    profile_id = profiles[0].get("permission_id") or profiles[0].get("id")
                self.logger.info("24Six: check-existing-user profile_id=%s", profile_id)
        except aiohttp.ClientError as exc:
            self.logger.warning("24Six: check-existing-user failed: %s", exc)

        # Step 3: POST /profiles/pin-check to pre-select profile
        if profile_id:
            xsrf = self._xsrf_header(session)
            try:
                async with session.post(
                    f"{BASE_URL}/profiles/pin-check",
                    json={"profile_id": profile_id},
                    headers=xsrf,
                ) as resp:
                    self.logger.info("24Six: pin-check status=%s", resp.status)
            except aiohttp.ClientError as exc:
                self.logger.warning("24Six: pin-check failed: %s", exc)

        # Step 4: POST /login with credentials to finalize auth
        xsrf = self._xsrf_header(session)
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

    async def _select_profile(self) -> None:
        """After login, POST /app/profile with empty body to finalize profile selection."""
        session = await self._get_session()
        xsrf = self._xsrf_header(session)
        try:
            async with session.post(
                f"{BASE_URL}/app/profile",
                headers=xsrf,
            ) as resp:
                body = await resp.text()
                self.logger.info("24Six: profile status=%s body=%s", resp.status, body[:200])
        except aiohttp.ClientError as exc:
            self.logger.warning("24Six: profile selection failed: %s", exc)

    async def _api_get(self, url: str, params: dict | None = None) -> dict:
        """Authenticated GET, auto-retry once on 401."""
        session = await self._get_session()
        # Inertia apps return JSON when X-Inertia header is present
        # Do NOT send X-Inertia-Version — version mismatch causes a 409 redirect
        inertia_headers = {
            "X-Inertia": "true",
        }
        try:
            async with session.get(url, params=params, headers=inertia_headers) as resp:
                if resp.status == 401:
                    self.logger.warning("24Six: 401 on GET %s — re-logging in", url)
                    await self._login()
                    async with session.get(url, params=params, headers=inertia_headers) as resp2:
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
    # Browse
    # ------------------------------------------------------------------

    async def browse(self, path: str) -> list[BrowseFolder | Album]:
        """Browse the 24Six featured homepage, organised by category."""
        parts = path.split("://", 1)
        sub = parts[1].lstrip("/") if len(parts) > 1 else ""

        # Fetch homepage data (plain XHR, no Inertia header)
        session = await self._get_session()
        homepage: list[dict] = []
        try:
            async with session.get(
                f"{BASE_URL}/app/music/featured-homepage"
            ) as resp:
                resp.raise_for_status()
                homepage = await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            self.logger.error("24Six: featured-homepage error: %s", exc)

        if not sub:
            # Root → one BrowseFolder per category
            items: list[BrowseFolder | Album] = []
            for section in homepage:
                cat = section.get("category", {})
                cat_id = str(cat.get("id", ""))
                cat_title = cat.get("title") or cat.get("title_hebrew") or cat_id
                cat_img = self._img_url(cat.get("img"))
                folder = BrowseFolder(
                    item_id=f"category_{cat_id}",
                    provider=self.instance_id,
                    path=f"{self.instance_id}://category/{cat_id}",
                    name=cat_title,
                )
                if cat_img:
                    folder.metadata.images = [
                        MediaItemImage(type=ImageType.THUMB, path=cat_img, provider=self.instance_id)
                    ]
                items.append(folder)
            return items

        # Category subfolder → albums for that category
        if sub.startswith("category/"):
            cat_id = sub.split("/", 1)[1]
            for section in homepage:
                if str(section.get("category", {}).get("id", "")) == cat_id:
                    return [
                        self._parse_album(item)
                        for item in section.get("data", [])
                        if item.get("type") == "collection"
                    ]

        return []

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(
        self,
        search_query: str,
        media_types: list[MediaType] | None = None,
        limit: int = 20,
    ) -> SearchResults:
        """Search 24Six using the Inertia GET search endpoint."""
        data = await self._api_get(
            f"{BASE_URL}/app/music/search",
            params={"q": search_query},
        )

        # _api_get returns {} on error; Inertia response is {props: {...}}
        # Log first 500 chars of response to diagnose unexpected formats
        self.logger.info("24Six: search raw type=%s keys=%s snippet=%s",
            type(data).__name__,
            list(data.keys()) if isinstance(data, dict) else "n/a",
            str(data)[:300],
        )
        if not isinstance(data, dict):
            self.logger.warning("24Six: search returned unexpected type %s", type(data).__name__)
            return SearchResults()

        props = data.get("props", {})
        if not isinstance(props, dict):
            props = {}

        results = SearchResults()

        if not media_types or MediaType.ARTIST in media_types:
            tiles = props.get("artists", {})
            if isinstance(tiles, dict):
                tiles = tiles.get("tiles", [])
            for item in (tiles or [])[:limit]:
                results.artists.append(self._parse_artist(item))

        if not media_types or MediaType.ALBUM in media_types:
            tiles = props.get("collections", {})
            if isinstance(tiles, dict):
                tiles = tiles.get("tiles", [])
            for item in (tiles or [])[:limit]:
                results.albums.append(self._parse_album(item))

        if not media_types or MediaType.TRACK in media_types:
            tiles = props.get("content", {})
            if isinstance(tiles, dict):
                tiles = tiles.get("tiles", [])
            for item in (tiles or [])[:limit]:
                results.tracks.append(self._parse_track(item))

        return results

    # ------------------------------------------------------------------
    # Artists
    # ------------------------------------------------------------------

    async def get_library_artists(self) -> AsyncGenerator[Artist, None]:
        return
        yield  # make this an async generator

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
        return
        yield  # make this an async generator

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
        return
        yield  # make this an async generator

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
        """Call POST /app/content/{content_id}/begin to get a signed Mux HLS URL."""
        cached = self._stream_url_cache.get(content_id)
        if cached:
            mux_url, expiry = cached
            if time.time() < expiry - TOKEN_REFRESH_BUFFER:
                return mux_url

        url = f"{BEGIN_ENDPOINT}/{content_id}/begin"
        body = {"device_id": self._device_id, "interaction": True}
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
        return mux_url

    async def get_stream_details(self, item_id: str) -> StreamDetails:
        """Return HLS stream details for ffmpeg."""
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
    """Decode the 'exp' claim from the JWT in a Mux signed URL."""
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
