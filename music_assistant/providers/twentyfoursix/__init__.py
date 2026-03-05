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
BEGIN_ENDPOINT = f"{BASE_URL}/app/content"
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
    _bearer_token: str = ""
    _profile_id: int = 0
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
                    "User-Agent": "TFS-Android/66.2.6",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-DEVICE-ID": self._device_id,
                    "X-DEVICE-NAME": "Music Assistant",
                    "X-PLATFORM-KEY": "production-android-44fd2f70",
                    "X-PLATFORM-DEVICE": "android",
                    "platform": "android",
                }
            )
        return self._session

    def _auth_headers(self) -> dict[str, str]:
        if self._bearer_token:
            return {"Authorization": f"Bearer {self._bearer_token}"}
        return {}

    async def _login(self) -> None:
        """Login via REST API v3."""
        import json as _json
        username: str = self.config.get_value(CONF_USERNAME)
        password: str = self.config.get_value(CONF_PASSWORD)
        session = await self._get_session()

        if not self._profile_id:
            self._profile_id = 89214
        for endpoint in ["profile-list", "profile/list"]:
            try:
                async with session.get(
                    f"{BASE_URL}/api/v3/{endpoint}",
                    json={"email": username, "password": password},
                ) as resp:
                    body = await resp.text()
                    self.logger.info("24Six: pre-auth %s status=%s body=%s", endpoint, resp.status, body[:400])
                    if resp.status == 200:
                        data = _json.loads(body)
                        profiles = data if isinstance(data, list) else (data.get("data") or data.get("profiles") or [])
                        for p in (profiles if isinstance(profiles, list) else []):
                            self.logger.info("24Six: profile id=%s name=%s", p.get("id"), p.get("name"))
                            if p.get("id") == 89214 or "chanoch" in str(p.get("name", "")).lower():
                                self._profile_id = int(p.get("id", 89214))
                                break
                        break
            except aiohttp.ClientError as exc:
                self.logger.warning("24Six: pre-auth %s failed: %s", endpoint, exc)

        try:
            async with session.post(
                f"{BASE_URL}/api/v3/login",
                json={"email": username, "password": password, "profile_id": self._profile_id},
            ) as resp:
                body = await resp.text()
                self.logger.info("24Six: api/v3/login status=%s body=%s", resp.status, body[:400])
                if resp.status not in (200, 201):
                    raise LoginFailed(f"24Six login failed HTTP {resp.status}. Check username/password.")
                data = _json.loads(body)
                self._bearer_token = (
                    data.get("token") or
                    data.get("access_token") or
                    (data.get("data") or {}).get("token") or
                    (data.get("data") or {}).get("access_token") or ""
                )
                self.logger.info("24Six: bearer token length=%s", len(self._bearer_token))
        except aiohttp.ClientError as exc:
            raise LoginFailed(f"24Six login request failed: {exc}") from exc

    async def _api_get(self, url: str, params: dict | None = None) -> dict:
        """Authenticated GET against REST API v3."""
        import json as _json
        session = await self._get_session()
        try:
            async with session.get(url, params=params, headers=self._auth_headers()) as resp:
                if resp.status == 401:
                    self.logger.warning("24Six: 401 on %s — re-logging in", url)
                    await self._login()
                    async with session.get(url, params=params, headers=self._auth_headers()) as resp2:
                        body = await resp2.text()
                        self.logger.info("24Six: retry GET %s status=%s body=%s", url.replace(BASE_URL,""), resp2.status, body[:300])
                        return _json.loads(body) if body else {}
                body = await resp.text()
                self.logger.info("24Six: GET %s status=%s body=%s", url.replace(BASE_URL,""), resp.status, body[:1500])
                return _json.loads(body) if body else {}
        except aiohttp.ClientError as exc:
            self.logger.warning("24Six: GET %s failed: %s", url, exc)
            return {}

    async def _api_post(self, url: str, body: dict | None = None) -> dict:
        """Authenticated POST against REST API v3."""
        import json as _jsonp
        session = await self._get_session()
        try:
            async with session.post(url, json=body or {}, headers=self._auth_headers()) as resp:
                if resp.status == 401:
                    self.logger.warning("24Six: 401 on POST %s — re-logging in", url)
                    await self._login()
                    async with session.post(url, json=body or {}, headers=self._auth_headers()) as resp2:
                        rb = await resp2.text()
                        return _jsonp.loads(rb) if rb else {}
                rb = await resp.text()
                self.logger.info("24Six: POST %s status=%s body=%s", url.replace(BASE_URL,""), resp.status, rb[:400])
                return _jsonp.loads(rb) if rb else {}
        except aiohttp.ClientError as exc:
            self.logger.warning("24Six: POST %s failed: %s", url, exc)
            return {}

    DASHBOARD_SECTIONS = [
        ("trending",      "Trending Now 🔥"),
        ("releases",      "New Releases"),
        ("featured",      "Featured"),
        ("newAlbums",     "New Albums"),
        ("newSingles",    "New Singles"),
        ("newArtists",    "New Artists"),
        ("artists",       "Top Artists"),
        ("playlists",     "Playlists"),
        ("by24Six",       "By 24Six"),
        ("recent",        "Recently Added"),
        ("femaleArtists", "Female Artists"),
        ("categories",    "Browse Categories"),
    ]

    _dashboard_cache: dict = {}

    async def browse(self, path: str) -> list[BrowseFolder | Album | Artist]:
        """Browse 24Six mirroring the app homepage structure."""
        parts = path.split("://", 1)
        sub = parts[1].lstrip("/") if len(parts) > 1 else ""

        if not sub:
            raw = await self._api_get(f"{BASE_URL}/api/v3/music")
            self._dashboard_cache = raw
            self.logger.info("24Six: dashboard keys=%s", list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__)
            items: list[BrowseFolder] = []
            for tile_type, label in self.DASHBOARD_SECTIONS:
                val = raw.get(tile_type) if isinstance(raw, dict) else None
                if val is None or (isinstance(val, list) and len(val) == 0):
                    continue
                items.append(BrowseFolder(
                    item_id=f"section_{tile_type}",
                    provider=self.instance_id,
                    path=f"{self.instance_id}://section/{tile_type}",
                    name=label,
                ))
            items.append(BrowseFolder(
                item_id="section_library",
                provider=self.instance_id,
                path=f"{self.instance_id}://section/library",
                name="My Library ♪",
            ))
            return items

        if sub.startswith("section/"):
            tile_type = sub.split("/", 1)[1]
            if tile_type == "library":
                raw = await self._api_get(f"{BASE_URL}/api/v3/music/library")
                data = raw if isinstance(raw, list) else raw.get("data") or raw.get("items") or []
            elif tile_type == "categories":
                data = self._dashboard_cache.get("categories") or []
                if not data:
                    raw = await self._api_get(f"{BASE_URL}/api/v3/music/category")
                    data = raw if isinstance(raw, list) else raw.get("data") or []
            else:
                data = self._dashboard_cache.get(tile_type)
                if data is None:
                    raw = await self._api_get(f"{BASE_URL}/api/v3/music")
                    self._dashboard_cache = raw
                    data = raw.get(tile_type) or []

            self.logger.info("24Six: section/%s count=%s", tile_type, len(data) if isinstance(data, list) else "?")
            results: list = []
            if not isinstance(data, list):
                return results
            for item in data:
                if not isinstance(item, dict):
                    continue
                item_type = item.get("type", "")
                if item_type == "artist" or "collection_count" in item:
                    results.append(self._parse_artist(item))
                elif item_type in ("collection", "album") or "track_count" in item:
                    results.append(self._parse_album(item))
                elif item_type in ("content", "track", "song"):
                    results.append(self._parse_track(item))
                elif item_type == "category":
                    results.append(BrowseFolder(
                        item_id=f"category_{item.get('id')}",
                        provider=self.instance_id,
                        path=f"{self.instance_id}://category/{item.get('id')}",
                        name=item.get("title") or item.get("name") or str(item.get("id")),
                    ))
                elif item_type == "playlist":
                    results.append(self._parse_album(item))
                else:
                    if "img" in item and "title" in item:
                        results.append(self._parse_album(item))
            return results

        if sub.startswith("section/category/"):
            cat_id = sub.split("/")[2]
            raw = await self._api_get(f"{BASE_URL}/api/v3/music/category", params={"id": cat_id})
            data = raw if isinstance(raw, list) else raw.get("data") or raw.get("tiles") or []
            return [self._parse_album(item) for item in data if isinstance(item, dict)]

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
        """Search 24Six."""
        import json as _json2
        session = await self._get_session()
        try:
            async with session.post(
                f"{BASE_URL}/api/v3/music/search",
                json={"q": search_query, "query": search_query},
                headers=self._auth_headers(),
            ) as resp:
                body = await resp.text()
                self.logger.info("24Six: POST search status=%s body=%s", resp.status, body[:1000])
                data = _json2.loads(body) if body else {}
        except Exception as exc:
            self.logger.warning("24Six: search POST failed: %s", exc)
            data = {}

        if not isinstance(data, dict):
            return SearchResults()
        props = data
        if "data" in data and isinstance(data["data"], dict):
            props = data["data"]

        results = SearchResults()
        if not media_types or MediaType.ARTIST in media_types:
            tiles = props.get("artists", [])
            if isinstance(tiles, dict):
                tiles = tiles.get("tiles", []) or tiles.get("data", [])
            for item in (tiles or [])[:limit]:
                results.artists.append(self._parse_artist(item))
        if not media_types or MediaType.ALBUM in media_types:
            tiles = props.get("collections") or props.get("albums") or []
            if isinstance(tiles, dict):
                tiles = tiles.get("tiles", [])
            for item in (tiles or [])[:limit]:
                results.albums.append(self._parse_album(item))
        if not media_types or MediaType.TRACK in media_types:
            tiles = props.get("songs") or props.get("content") or []
            if isinstance(tiles, dict):
                tiles = tiles.get("tiles", []) or tiles.get("data", [])
            for item in (tiles or [])[:limit]:
                results.tracks.append(self._parse_track(item))
        return results

    # ------------------------------------------------------------------
    # Artists
    # ------------------------------------------------------------------

    async def get_library_artists(self) -> AsyncGenerator[Artist, None]:
        return
        yield

    async def get_artist(self, prov_artist_id: str) -> Artist:
        data = await self._api_get(f"{BASE_URL}/api/v3/music/artist/{prov_artist_id}")
        artist_data = data.get("artist") or data
        if not artist_data or not artist_data.get("id"):
            raise MediaNotFoundError(f"Artist {prov_artist_id} not found on 24Six")
        return self._parse_artist(artist_data)

    async def get_artist_albums(self, prov_artist_id: str) -> list[Album]:
        data = await self._api_get(f"{BASE_URL}/api/v3/music/artist/{prov_artist_id}")
        collections = (data.get("collections") or data.get("artist_albums") or
                       data.get("albums") or data.get("latest_collections") or [])
        if isinstance(collections, dict):
            collections = collections.get("tiles") or collections.get("data") or []
        if not collections:
            coll_data = await self._api_get(
                f"{BASE_URL}/api/v3/music/artist/{prov_artist_id}",
                params={"include": "collections"}
            )
            collections = (coll_data.get("collections") or coll_data.get("artist_albums") or [])
        self.logger.info("24Six: artist %s albums count=%s", prov_artist_id, len(collections))
        return [self._parse_album(c) for c in collections if isinstance(c, dict)]

    async def get_artist_toptracks(self, prov_artist_id: str) -> list[Track]:
        data = await self._api_get(f"{BASE_URL}/api/v3/music/artist/{prov_artist_id}")
        tracks = data.get("top_songs") or data.get("content") or []
        if isinstance(tracks, dict):
            tracks = tracks.get("tiles") or tracks.get("data") or []
        if not tracks:
            latest = data.get("latest")
            if isinstance(latest, dict) and latest.get("id"):
                tracks = [latest]
        self.logger.info("24Six: artist %s top tracks count=%s", prov_artist_id, len(tracks))
        return [self._parse_track(t) for t in tracks if isinstance(t, dict)]

    # ------------------------------------------------------------------
    # Albums
    # ------------------------------------------------------------------

    async def get_library_albums(self) -> AsyncGenerator[Album, None]:
        return
        yield

    async def get_album(self, prov_album_id: str) -> Album:
        data = await self._api_get(f"{BASE_URL}/api/v3/music/collection/{prov_album_id}")
        album_data = data.get("collection") or data
        if not album_data or not album_data.get("id"):
            raise MediaNotFoundError(f"Album {prov_album_id} not found on 24Six")
        return self._parse_album(album_data)

    async def get_album_tracks(self, prov_album_id: str) -> list[Track]:
        data = await self._api_get(f"{BASE_URL}/api/v3/music/collection/{prov_album_id}")
        collection_meta = data.get("collection") or {}
        tracks = (data.get("contents") or data.get("content") or
                  data.get("tracks") or collection_meta.get("contents") or [])
        if isinstance(tracks, dict):
            tracks = tracks.get("tiles") or tracks.get("data") or []
        self.logger.error("24Six: album %s tracks count=%s", prov_album_id, len(tracks))
        result = []
        for t in tracks:
            if not isinstance(t, dict):
                continue
            if not t.get("collection") and collection_meta.get("id"):
                t = {**t, "collection": collection_meta}
            result.append(self._parse_track(t))
        return result

    # ------------------------------------------------------------------
    # Tracks
    # ------------------------------------------------------------------

    async def get_library_tracks(self) -> AsyncGenerator[Track, None]:
        return
        yield

    async def get_track(self, prov_track_id: str) -> Track:
        """Fetch single track metadata."""
        import json as _jt
        session = await self._get_session()
        try:
            async with session.post(
                f"{BASE_URL}/api/v3/music/content/{prov_track_id}",
                json={},
                headers=self._auth_headers(),
            ) as resp:
                body = await resp.text()
                self.logger.error("24Six: GET_TRACK POST content/%s status=%s top_keys=%s",
                    prov_track_id, resp.status,
                    list(_jt.loads(body).keys()) if body and body[0]=='{' else body[:100])
                data = _jt.loads(body) if body else {}
        except Exception as exc:
            raise MediaNotFoundError(f"Track {prov_track_id} fetch failed: {exc}") from exc
        track_data = data if data.get("id") else (data.get("content") or data)
        if not track_data or not track_data.get("id"):
            raise MediaNotFoundError(f"Track {prov_track_id} not found on 24Six")
        return self._parse_track(track_data)

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    async def _begin_stream(self, content_id: str, audio_format: str = "m4a") -> str:
        """Construct stream URL per TfsMediaSource APK bytecode."""
        cached = self._stream_url_cache.get(content_id)
        if cached:
            stream_url, expiry = cached
            if time.time() < expiry - TOKEN_REFRESH_BUFFER:
                return stream_url

        stream_url = (
            f"https://24six.app/api/v3/content/{content_id}"
            f"/play?format={audio_format}"
        )
        self.logger.info("24Six: constructed stream URL: %s", stream_url)

        expiry = int(time.time()) + 6 * 3600
        self._stream_url_cache[content_id] = (stream_url, expiry)
        return stream_url

    async def get_stream_details(self, item_id: str, media_item=None) -> StreamDetails:
        """Return stream details with Bearer auth header passed to ffmpeg via http_headers."""
        self.logger.info("24Six: get_stream_details called for item_id=%s", item_id)

        import json as _js
        audio_fmt = "m4a"
        content_type = ContentType.AAC
        stream_type = StreamType.HTTP

        try:
            session = await self._get_session()
            async with session.post(
                f"{BASE_URL}/api/v3/music/content/{item_id}",
                json={},
                headers=self._auth_headers(),
            ) as resp:
                body = await resp.text()
                if resp.status == 200:
                    data = _js.loads(body) if body else {}
                    raw_fmt = data.get("audio_format", "m4a")
                    if isinstance(raw_fmt, str):
                        audio_fmt = raw_fmt.lower()
                    self.logger.info("24Six: content metadata audio_format=%r", audio_fmt)
        except Exception as exc:
            self.logger.warning("24Six: failed to fetch content metadata: %s", exc)

        if audio_fmt == "m3u8":
            content_type = ContentType.HLS
            stream_type = StreamType.HLS
        elif audio_fmt == "ogg":
            content_type = ContentType.OGG
        else:
            content_type = ContentType.AAC

        stream_url = await self._begin_stream(item_id, audio_fmt)
        self.logger.info("24Six: streaming %s fmt=%s url=%s", item_id, audio_fmt, stream_url)

        return StreamDetails(
            item_id=item_id,
            provider=self.instance_id,
            audio_format=AudioFormat(content_type=content_type),
            stream_type=stream_type,
            path=stream_url,
            http_headers={"Authorization": f"Bearer {self._bearer_token}"},
        )

    # ------------------------------------------------------------------
    # Data mapping helpers
    # ------------------------------------------------------------------

    def _img_url(self, raw_url: str | None) -> str | None:
        if not raw_url:
            return None
        return raw_url.split("?")[0]

    def _parse_artist(self, data: dict) -> Artist:
        artist_id = str(data.get("id") or data.get("artist_id") or "")
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
        album_id = str(data.get("id") or data.get("collection_id") or "")
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
        track_id = str(data.get("id") or data.get("content_id") or "")
        collection = data.get("collection") or {}
        raw_artists = data.get("artists") or []
        if not raw_artists and data.get("artist_id"):
            raw_artists = [{"id": data["artist_id"], "name": data.get("subtitle", "").split(",")[0].strip() or "Unknown"}]
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
                for a in raw_artists if a.get("id")
            ],
            album=(
                ItemMapping(
                    item_id=str(collection.get("id") or data.get("collection_id", "")),
                    provider=self.instance_id,
                    name=collection.get("title", ""),
                )
                if (collection.get("id") or data.get("collection_id"))
                else None
            ),
            duration=data.get("length", 0),
            track_number=data.get("track_num"),
            provider_mappings={
                ProviderMapping(
                    item_id=track_id,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    audio_format=AudioFormat(content_type=ContentType.AAC),
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
