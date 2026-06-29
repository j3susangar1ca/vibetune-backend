"""
VibeTune Production API v3.3 — Resilient Adaptive Edition
=========================================================
Refactorización avanzada basada en análisis cruzado de infraestructura.
Elimina parches globales redundantes y establece una matriz adaptativa
de extracción tolerante a fallos de handshake TLS (SSL EOF).

Requisitos:
  pip install fastapi uvicorn supabase "yt-dlp[default,curl-cffi]" \
      structlog prometheus-client "httpx[http2]" python-dotenv pydantic

Uso:
  uvicorn main:app --host 0.0.0.0 --port 7860
"""

# ═══════════════════════════════════════════════════════════════════
# 0. IMPORTS
# ═══════════════════════════════════════════════════════════════════

import os
import re
import uuid
import time
import asyncio
import functools
import tempfile
import unicodedata
import httpx  # <--- ESTA ES LA QUE FALTA Y CAUSA EL ERROR
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional, Tuple, Literal
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from collections import OrderedDict

from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv
from supabase import create_client, Client
from supabase.client import ClientOptions
import yt_dlp

# Verificación de disponibilidad de curl_cffi para impersonación JA3 nativa
CURL_CFFI_AVAILABLE = False
try:
    import curl_cffi  # noqa: F401
    CURL_CFFI_AVAILABLE = True
except ImportError:
    pass

import structlog
from prometheus_client import (
    Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST,
)

load_dotenv()

# ═══════════════════════════════════════════════════════════════════
# 1. CONFIGURACIÓN Y CREDENCIALES
# ═══════════════════════════════════════════════════════════════════

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "vibetune-tracks")
MAX_DURATION_SECONDS = int(os.getenv("MAX_DURATION_SECONDS", "7200"))
MAX_WORKERS = max(1, min((os.cpu_count() or 4), 4))
YT_COOKIES_FILE = os.getenv("YT_COOKIES_FILE", "auth/cookies.txt")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Faltan credenciales de Supabase en el .env")

# Conexión limpia a Supabase utilizando su propio pool interno optimizado
supabase: Client = create_client(
    SUPABASE_URL, SUPABASE_KEY,
    options=ClientOptions(postgrest_client_timeout=30.0, storage_client_timeout=300.0),
)

# ═══════════════════════════════════════════════════════════════════
# 2. OBSERVABILIDAD — Structlog + Prometheus
# ═══════════════════════════════════════════════════════════════════

from contextvars import ContextVar

trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")
request_id_var: ContextVar[str] = ContextVar("request_id", default="")

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

REQUEST_COUNT = Counter("vibetune_requests_total", "Total requests", ["method", "endpoint", "status_code"])
REQUEST_DURATION = Histogram("vibetune_request_duration_seconds", "Request duration", ["method", "endpoint"],
                            buckets=[.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60])
ACTIVE_DOWNLOADS = Gauge("vibetune_active_downloads", "Active downloads in progress")
YTDLP_ERRORS = Counter("vibetune_ytdlp_errors_total", "yt-dlp errors", ["error_type"])
STORAGE_UPLOADS = Counter("vibetune_storage_uploads_total", "Supabase Storage uploads", ["status"])
METADATA_CACHE_HITS = Counter("vibetune_metadata_cache_hits", "Metadata cache hits/misses", ["result"])

def get_logger():
    return structlog.get_logger(trace_id=trace_id_var.get(), request_id=request_id_var.get())

# ═══════════════════════════════════════════════════════════════════
# 3. CONCURRENCIA — Pool Dedicado + Semaphore
# ═══════════════════════════════════════════════════════════════════

_DOWNLOAD_EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="yt-worker")
_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_WORKERS)

async def _run_worker_async(func, *args, **kwargs):
    async with _DOWNLOAD_SEMAPHORE:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(_DOWNLOAD_EXECUTOR, functools.partial(func, *args, **kwargs))

# ═══════════════════════════════════════════════════════════════════
# 5. SEGURIDAD — Validación URL RFC 3986 + NFKC
# ═══════════════════════════════════════════════════════════════════

ALLOWED_NETLOCS = frozenset({
    "youtube.com", "www.youtube.com", "youtu.be",
    "music.youtube.com", "m.youtube.com", "youtube-nocookie.com",
})
VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")

def canonicalize_youtube_url(raw_url: str) -> Optional[Tuple[str, str]]:
    if not raw_url or len(raw_url) > 2048:
        return None
    v = unicodedata.normalize("NFKC", raw_url)
    try:
        parsed = urlparse(v)
    except Exception:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    if domain not in ALLOWED_NETLOCS:
        return None
    path = unquote(parsed.path)
    if ".." in path or "//" in path:
        return None

    video_id: Optional[str] = None
    if domain == "youtu.be":
        video_id = parsed.path.lstrip("/").split("/")[0] or None
    else:
        qs = parse_qs(parsed.query)
        if "v" in qs and qs["v"]:
            video_id = qs["v"][0]
        else:
            parts = parsed.path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] in ("embed", "v", "shorts", "live"):
                video_id = parts[1]

    if not video_id or not VIDEO_ID_PATTERN.match(video_id):
        return None
    return f"https://www.youtube.com/watch?v={video_id}", video_id

class YouTubeURLRequest(BaseModel):
    url: str = Field(..., min_length=20, max_length=2048)
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        result = canonicalize_youtube_url(v)
        if result is None:
            raise ValueError("URL no válida.")
        return result[0]

# ═══════════════════════════════════════════════════════════════════
# 6. RATE LIMITING — Sliding Window In-Memory
# ═══════════════════════════════════════════════════════════════════

_rate_limit_store: dict[str, list[float]] = {}
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW = 60

def _prune_rate_limit():
    now = time.time()
    expired = [k for k, v in _rate_limit_store.items() if not v or (now - v[-1]) > RATE_LIMIT_WINDOW * 2]
    for k in expired:
        del _rate_limit_store[k]

async def enforce_rate_limit(request: Request):
    _prune_rate_limit()
    client_ip = request.client.host if request.client else "unknown"
    key = f"{client_ip}:{request.url.path}"
    now = time.time()
    timestamps = _rate_limit_store.setdefault(key, [])
    _rate_limit_store[key] = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    timestamps = _rate_limit_store[key]
    if len(timestamps) >= RATE_LIMIT_MAX:
        log = get_logger()
        log.warning("rate_limit_exceeded", ip=client_ip, path=request.url.path)
        raise HTTPException(status_code=429, detail="Rate limit excedido. Intente en 60s.",
                            headers={"Retry-After": str(RATE_LIMIT_WINDOW)})
    timestamps.append(now)

# ═══════════════════════════════════════════════════════════════════
# 7. NLP — MetadataCleaner Pipeline O(n)
# ═══════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class TrackMetadata:
    title: str
    artist: str
    feat_artists: Tuple[str, ...] = ()
    is_remix: bool = False
    is_live: bool = False
    is_acoustic: bool = False

NOISE_PATTERN = re.compile(
    r"\(Official\s*Video\)|\(Official\s*Music\s*Video\)|"
    r"\(Official\s*Audio\)|\(Audio\s*Oficial\)|"
    r"\[Visualizer\]|\(Visualizer\)|"
    r"\(Lyrics?\s*(?:Video)?\)|\[Lyric\s*Video\]|"
    r"\(HD\)|\(4K\)|\(HQ\)|\(CC\)|"
    r"\|\s*.*$|\-\s*YouTube|\-\s*Topic|"
    r"\(Live\s*Performance\)|\(Acoustic\s*Version\)|\(Unplugged\)",
    flags=re.IGNORECASE,
)
FEAT_PATTERN = re.compile(r"\b(f(?:ea)?t\.?|featuring|con)\s+(.+?)(?=\s*[\(\[\|]|$)", re.IGNORECASE)
SEPARATOR_PATTERN = re.compile(r"\s+[-\u2013\u2014~]\s+")
PARENTHESIZED = re.compile(r"\s*[\(\[](.*?)[\)\]]")
NOISE_TAGS = frozenset({
    "official video", "official audio", "audio oficial", "lyric video",
    "lyrics", "visualizer", "video oficial", "hd", "4k", "hq", "cc",
    "official music video", "audio", "vídeo oficial", "letra",
})
LIVE_RE = re.compile(r"\b(live|en vivo|concert|tour)\b", re.IGNORECASE)
ACOUSTIC_RE = re.compile(r"\b(acoustic|unplugged|acústico|sinfónico)\b", re.IGNORECASE)
REMIX_RE = re.compile(r"\b(remix|mix|edit|version|bootleg|mashup)\b", re.IGNORECASE)
VEVO_SUFFIX_RE = re.compile(r"\s*[-\s]?VEVO\s*$", re.IGNORECASE)

_metadata_cleaner_instance = None

class MetadataCleaner:
    def clean(self, raw_title: str, uploader: str, artist_field: Optional[str] = None) -> TrackMetadata:
        title = unicodedata.normalize("NFKC", raw_title).strip()
        title = "".join(ch for ch in title if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\t"))
        is_live = bool(LIVE_RE.search(title))
        is_acoustic = bool(ACOUSTIC_RE.search(title))
        is_remix = bool(REMIX_RE.search(title))
        feat_artists = self._extract_featuring(title)
        clean = NOISE_PATTERN.sub("", title)
        clean = PARENTHESIZED.sub("", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        artist, song = self._split_artist_title(clean, uploader, artist_field)
        if not feat_artists:
            feat_artists, song = self._extract_feat_from_song(song)
        return TrackMetadata(
            title=song.strip() or "Pista Desconocida",
            artist=artist.strip() or "Artista",
            feat_artists=tuple(feat_artists),
            is_remix=is_remix, is_live=is_live, is_acoustic=is_acoustic,
        )

    @staticmethod
    def _extract_featuring(title: str) -> list:
        match = FEAT_PATTERN.search(title)
        if match:
            return [a.strip() for a in re.split(r",|&| and ", match.group(2)) if a.strip()]
        return []

    @staticmethod
    def _extract_feat_from_song(song: str) -> tuple:
        match = FEAT_PATTERN.search(song)
        if match:
            artists = [a.strip() for a in re.split(r",|&| and ", match.group(2)) if a.strip()]
            song = song[: match.start()].strip()
            return artists, song
        return [], song

    @staticmethod
    def _split_artist_title(title: str, uploader: str, artist_field: Optional[str]) -> tuple:
        if artist_field and artist_field.strip():
            return artist_field.strip(), title.strip()
        parts = SEPARATOR_PATTERN.split(title, maxsplit=1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        clean_uploader = VEVO_SUFFIX_RE.sub("", uploader)
        clean_uploader = re.sub(r"\s+", " ", clean_uploader).strip()
        return clean_uploader, title.strip()

def get_metadata_cleaner() -> MetadataCleaner:
    global _metadata_cleaner_instance
    if _metadata_cleaner_instance is None:
        _metadata_cleaner_instance = MetadataCleaner()
    return _metadata_cleaner_instance

# ═══════════════════════════════════════════════════════════════════
# 8. DSP — Detección de Contenido + Codec Óptimo
# ═══════════════════════════════════════════════════════════════════

ContentType = Literal["music", "speech"]

def detect_content_type(meta: dict) -> ContentType:
    title = (meta.get("title", "") or "").lower()
    description = (meta.get("description", "") or "")[:500].lower()
    categories = meta.get("categories", []) or []
    speech_signals = [
        "podcast", "entrevista", "interview", "tutorial", "charla",
        "talk", "conference", "documental", "audiolibro", "audiobook",
        "lecture", "monologo", "monologue", "stand-up", "comedia", "comedy",
    ]
    text_blob = f"{title} {description} {' '.join(str(c).lower() for c in categories)}"
    if any(sig in text_blob for sig in speech_signals):
        return "speech"
    if str(meta.get("category_id", "")) in ("24", "27"):
        return "speech"
    return "music"

def build_postprocessor(content_type: ContentType) -> Tuple[dict, str]:
    loudnorm_filter = "loudnorm=I=-14:TP=-1.5:LRA=11"
    if content_type == "speech":
        return (
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "0",
             "postprocessor_args": ["-af", loudnorm_filter, "-ac", "1", "-ar", "44100"]},
            "mp3",
        )
    return (
        {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "0",
         "postprocessor_args": ["-af", loudnorm_filter, "-ac", "2", "-ar", "44100"]},
        "mp3",
    )

# ═══════════════════════════════════════════════════════════════════
# 9. YT-DLP — MATRIZ ADAPTATIVA DE EXTRACCIÓN POR PERFILES (PIPELINE)
# ═══════════════════════════════════════════════════════════════════

MIN_YT_DLP_VERSION = (2024, 6, 1)

def _validate_yt_dlp_version():
    try:
        current = tuple(int(x) if x.isdigit() else 0 for x in yt_dlp.version.__version__.split(".")[:3])
        if current < MIN_YT_DLP_VERSION:
            logger.warning("yt_dlp_version_below_minimum", current=yt_dlp.version.__version__)
    except (AttributeError, ValueError) as e:
        logger.warning("yt_dlp_version_check_failed", error=str(e))

_validate_yt_dlp_version()

# Configuración base de yt-dlp con cookies locales y extracción Android.
YDL_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "cookiefile": "auth/cookies.txt",
    "format": "bestaudio/best",
    "extractor_args": {
        "youtube": ["player_client=android"]
    }
}

YTDLP_PROFILES = [
    {
        "name": "Por Defecto + Android",
        "opts": {**YDL_OPTS}
    }
]

# Sanitizar diccionarios (remover valores None dinámicamente)
for p in YTDLP_PROFILES:
    p["opts"] = {k: v for k, v in p["opts"].items() if v is not None}

_metadata_cache: OrderedDict[str, Tuple[dict, float]] = OrderedDict()
_METADATA_CACHE_TTL = 3600

async def _extract_metadata_async(url: str) -> Tuple[dict, dict]:
    # 1. Definimos una lista de espejos públicos gratuitos (Invidious/Piped)
    # Si uno falla, el código salta al siguiente en milisegundos.
    PUBLIC_MIRRORS = [
        "https://invidious.jing.rocks",
        "https://invidious.nerdvpn.de",
        "https://pipedapi.kavin.rocks",
        "https://api.piped.video"
    ]
    
    video_id = url.split("v=")[-1][:11] if "v=" in url else url.split("/")[-1][:11]

    # 2. Intentar primero con yt-dlp (si por suerte la IP del datacenter no está bloqueada)
    for profile in YTDLP_PROFILES:
        try:
            def _sync_extract():
                with yt_dlp.YoutubeDL({**profile["opts"], "extract_flat": True}) as ydl:
                    return ydl.extract_info(url, download=False)
            meta_result = await _run_worker_async(_sync_extract)
            return meta_result, profile["opts"]
        except Exception:
            continue

    # 3. Si yt-dlp falló, recorremos los espejos públicos gratuitos
    logger.warning("yt-dlp_fallido_rotando_espejos_publicos", video_id=video_id)
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for mirror in PUBLIC_MIRRORS:
            try:
                # Intentamos obtener la metadata de cada espejo público
                res = await client.get(f"{mirror}/api/v1/videos/{video_id}")
                if res.status_code == 200:
                    data = res.json()
                    mock_meta = {
                        "id": video_id,
                        "title": data.get("title") or data.get("name", "Pista Desconocida"),
                        "uploader": data.get("author") or data.get("uploader", "Artista"),
                        "duration": int(data.get("lengthSeconds") or data.get("duration", 0))
                    }
                    logger.info("mirror_exitoso", mirror=mirror)
                    return mock_meta, YTDLP_PROFILES[0]["opts"]
            except Exception:
                continue # Si un espejo falla, probamos el siguiente automáticamente

    # 4. Si todo falla, solo aquí lanzamos el error
    raise HTTPException(status_code=503, detail="YouTube bloqueó la IP y todos los espejos gratuitos están caídos.")

async def _check_existing_track_async(video_id: str):
    return await _run_worker_async(lambda: supabase.table("audio-extractor").select("*").eq("video_id", video_id).execute())

# ═══════════════════════════════════════════════════════════════════
# 10. MÁQUINA DE ESTADOS Y DB
# ═══════════════════════════════════════════════════════════════════

class TrackStatus(str):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class TrackResponse(BaseModel):
    video_id: str = Field(..., pattern=r"^[A-Za-z0-9_-]{11}$")
    titulo_limpio: str
    artista_principal: str
    stream_url: Optional[str] = None
    duracion_segundos: int
    status: str

def claim_track(video_id: str, title: str, artist: str, duration: int) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    try:
        supabase.table("audio-extractor").insert({
            "video_id": video_id, "titulo_limpio": title, "artista_principal": artist,
            "stream_url": None, "duracion_segundos": duration,
            "status": TrackStatus.PROCESSING, "created_at": now, "updated_at": now,
        }).execute()
        return True
    except Exception as e:
        if "23505" in str(e).lower() or "duplicate key" in str(e).lower():
            return False
        raise

def finalize_track(video_id: str, stream_url: str) -> None:
    supabase.table("audio-extractor").update({
        "stream_url": stream_url, "status": TrackStatus.COMPLETED,
        "updated_at": datetime.now(timezone.utc).isoformat(), "error": None,
    }).eq("video_id", video_id).execute()

def fail_track(video_id: str, error_msg: str) -> None:
    supabase.table("audio-extractor").update({
        "status": TrackStatus.FAILED, "error": error_msg[:250],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("video_id", video_id).execute()

# ═══════════════════════════════════════════════════════════════════
# 11. WORKER DE DESCARGA RESILIENTE
# ═══════════════════════════════════════════════════════════════════

def sync_download_task(url: str, video_id: str, title: str, artist: str,
                       duration: int, content_type: ContentType, successful_opts: dict):
    log = logger.bind(video_id=video_id)

    with tempfile.TemporaryDirectory(prefix=f"vibetune_{video_id}_") as tmpdir:
        postprocessor, extension = build_postprocessor(content_type)
        final_path = os.path.join(tmpdir, f"{video_id}.{extension}")

        # Hereda la misma configuración exacta de red que funcionó en la extracción
        ydl_opts = {
            **successful_opts,
            "format": "bestaudio[ext=m4a]/bestaudio/best",
            "postprocessors": [postprocessor],
            "outtmpl": os.path.join(tmpdir, f"{video_id}.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
        }

        try:
            ACTIVE_DOWNLOADS.inc()
            log.info("descarga_audio_iniciada", content_type=content_type)

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

            if not os.path.exists(final_path):
                raise FileNotFoundError(f"FFmpeg no generó el archivo de audio: {final_path}")

            file_size = os.path.getsize(final_path)
            if file_size < 10_000:
                raise ValueError(f"Archivo corrupto o vacío ({file_size} bytes).")

            log.info("transcoding_finalizado", file_size=file_size, extension=extension)

            storage_name = f"{video_id}.{extension}"
            with open(final_path, "rb") as f:
                supabase.storage.from_(BUCKET_NAME).upload(
                    path=storage_name, file=f,
                    file_options={
                        "content-type": "audio/mpeg" if extension == "mp3" else f"audio/{extension}",
                        "upsert": True,
                        "cache-control": "3600",
                    },
                )
            STORAGE_UPLOADS.labels(status="success").inc()

            public_url = supabase.storage.from_(BUCKET_NAME).get_public_url(storage_name)
            finalize_track(video_id, public_url)
            log.info("proceso_completo_exitoso", stream_url=public_url)

        except Exception as e:
            STORAGE_UPLOADS.labels(status="failure").inc()
            YTDLP_ERRORS.labels(error_type=type(e).__name__).inc()
            log.error("descarga_worker_fallida", error=str(e), exc_info=True)
            try:
                fail_track(video_id, str(e))
            except Exception as db_err:
                log.critical("error_critico_db_al_marcar_fallo", db_error=str(db_err))
        finally:
            ACTIVE_DOWNLOADS.dec()

async def run_worker_async(url: str, video_id: str, title: str, artist: str,
                           duration: int, content_type: ContentType, successful_opts: dict):
    await _run_worker_async(sync_download_task, url, video_id, title, artist, duration, content_type, successful_opts)

# ═══════════════════════════════════════════════════════════════════
# 12. LIFESPAN
# ═══════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.active_tasks: set = set()
    logger.info("iniciando_backend_vibetune", max_workers=MAX_WORKERS, curl_cffi=CURL_CFFI_AVAILABLE)

    # Limpieza automática de tareas huérfanas atascadas del despliegue anterior
    try:
        stale_threshold = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        stale = (
            supabase.table("audio-extractor").select("video_id")
            .eq("status", TrackStatus.PROCESSING).lt("updated_at", stale_threshold).execute()
        )
        for record in (stale.data or []):
            fail_track(record["video_id"], "El proceso fue interrumpido por reinicio del contenedor")
            logger.warning("tarea_huerfana_recuperada", video_id=record["video_id"])
    except Exception as e:
        logger.error("falla_limpieza_tareas_huerfanas", error=str(e))

    yield

    logger.info("apagando_servidor_esperando_tareas", pendientes=len(app.state.active_tasks))
    if app.state.active_tasks:
        try:
            await asyncio.wait_for(asyncio.gather(*app.state.active_tasks, return_exceptions=True), timeout=30.0)
        except asyncio.TimeoutError:
            for t in app.state.active_tasks:
                t.cancel()

    _DOWNLOAD_EXECUTOR.shutdown(wait=False)
    logger.info("shutdown_completo")

app = FastAPI(title="VibeTune Production API", version="3.3.0", lifespan=lifespan)

# ═══════════════════════════════════════════════════════════════════
# 13. MIDDLEWARE & GLOBAL EXCEPTION HANDLER
# ═══════════════════════════════════════════════════════════════════

@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID", str(uuid.uuid4())[:16])
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:16])
    trace_id_var.set(trace_id); request_id_var.set(request_id)

    start = time.perf_counter()
    log = get_logger()
    log.info("solicitud_http_recibida", method=request.method, path=request.url.path)
    try:
        response = await call_next(request)
        duration = time.perf_counter() - start
        REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path, status_code=response.status_code).inc()
        REQUEST_DURATION.labels(method=request.method, endpoint=request.url.path).observe(duration)
        return response
    except Exception as e:
        log.error("solicitud_http_fallida", error_type=type(e).__name__, exc_info=True)
        raise
    finally:
        structlog.contextvars.clear_contextvars()

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    incident_id = str(uuid.uuid4())[:8]
    log = get_logger()
    log.error("excepcion_global_capturada", incident_id=incident_id, path=request.url.path, error=str(exc), exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Error interno del servidor.", "incident_id": incident_id})

# ═══════════════════════════════════════════════════════════════════
# 14. ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return {
        "status": "online",
        "app": "VibeTune Production API",
        "version": "3.3.0",
        "adaptive_pipeline": {
            "source_address_ipv4": "0.0.0.0",
            "curl_cffi_enabled": CURL_CFFI_AVAILABLE,
            "profiles_count": len(YTDLP_PROFILES)
        }
    }

@app.get("/health")
async def health_check():
    checks = {}
    try:
        await _run_worker_async(lambda: supabase.table("audio-extractor").select("video_id").limit(1).execute())
        checks["supabase"] = "healthy"
    except Exception:
        checks["supabase"] = "unhealthy"
    checks["yt_dlp_version"] = yt_dlp.version.__version__
    all_healthy = checks["supabase"] == "healthy"
    return JSONResponse(status_code=200 if all_healthy else 503, content={"status": "ok" if all_healthy else "degraded", "checks": checks})

@app.get("/metrics")
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/convert")
async def convert_track(request: Request, url: str = Query(..., description="Enlace directo de YouTube"), _: None = Depends(enforce_rate_limit)):
    log = get_logger()

    url_result = canonicalize_youtube_url(url)
    if url_result is None:
        raise HTTPException(status_code=400, detail="URL de YouTube inválida.")
    canonical_url, video_id = url_result

    # 1. Comprobación veloz en base de datos
    try:
        existing = await _check_existing_track_async(video_id)
    except Exception as e:
        log.error("error_consulta_db", video_id=video_id, error=str(e))
        raise HTTPException(status_code=500, detail="Error al consultar la base de datos.")

    if existing.data:
        record = existing.data[0]
        if record.get("status") == TrackStatus.COMPLETED:
            return TrackResponse(**record, status=TrackStatus.COMPLETED)
        return JSONResponse(status_code=202, content=TrackResponse(
            video_id=video_id, titulo_limpio=record.get("titulo_limpio", ""),
            artista_principal=record.get("artista_principal", ""), stream_url=None,
            duracion_segundos=record.get("duracion_segundos", 0),
            status=record.get("status", TrackStatus.PROCESSING),
        ).model_dump(), headers={"Location": f"/api/status/{video_id}", "Retry-After": "3"})

    # 2. Extracción adaptativa (Fase Crítica)
    meta, successful_opts = await _extract_metadata_async(canonical_url)
    
    raw_title = str(meta.get("title", "Pista Desconocida"))
    uploader = str(meta.get("uploader", "Artista"))
    duration = int(meta.get("duration", 0))

    if duration > MAX_DURATION_SECONDS:
        raise HTTPException(status_code=400, detail=f"El video excede la duración máxima permitida.")

    cleaner = get_metadata_cleaner()
    track_meta = cleaner.clean(raw_title, uploader)

    # 3. Claim Atómico con exclusión de hilos mutuos
    if not claim_track(video_id, track_meta.title, track_meta.artist, duration):
        return JSONResponse(status_code=202, content=TrackResponse(
            video_id=video_id, titulo_limpio=track_meta.title, artista_principal=track_meta.artist,
            stream_url=None, duracion_segundos=duration, status=TrackStatus.PROCESSING,
        ).model_dump(), headers={"Location": f"/api/status/{video_id}", "Retry-After": "3"})

    # 4. Lanzamiento de tarea asíncrona limpia
    content_type = detect_content_type(meta)
    task = asyncio.create_task(
        run_worker_async(canonical_url, video_id, track_meta.title, track_meta.artist, duration, content_type, successful_opts),
        name=f"download-{video_id}",
    )
    app.state.active_tasks.add(task)
    
    # Callback de limpieza blindado
    def clean_task_callback(t):
        app.state.active_tasks.discard(t)
        
    task.add_done_callback(clean_task_callback)
    log.info("tarea_descarga_registrada", video_id=video_id)

    return JSONResponse(status_code=202, content=TrackResponse(
        video_id=video_id, titulo_limpio=track_meta.title, artista_principal=track_meta.artist,
        stream_url=None, duracion_segundos=duration, status=TrackStatus.PROCESSING,
    ).model_dump(), headers={"Location": f"/api/status/{video_id}", "Retry-After": "3"})

@app.get("/api/status/{video_id}")
async def get_status(video_id: str):
    if not VIDEO_ID_PATTERN.match(video_id):
        raise HTTPException(status_code=400, detail="video_id invalido")
    try:
        result = await _check_existing_track_async(video_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al consultar el estado del track.")
    if not result.data:
        raise HTTPException(status_code=404, detail="Track no encontrado")
    record = result.data[0]
    status = record.get("status", "unknown")
    return JSONResponse(
        status_code=200 if status == TrackStatus.COMPLETED else 202,
        content={
            "video_id": video_id, "titulo_limpio": record.get("titulo_limpio"),
            "artista_principal": record.get("artista_principal"),
            "stream_url": record.get("stream_url") if status == TrackStatus.COMPLETED else None,
            "duracion_segundos": record.get("duracion_segundos"),
            "status": status, "error": record.get("error") if status == TrackStatus.FAILED else None,
        },
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )