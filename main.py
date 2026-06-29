import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

app = FastAPI(title="VibeTune Backend - Stream Extractor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "service": "VibeTune Backend - Stream Extractor"}


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/extract")
def extract_stream_url(url: str = Query(..., description="URL de YouTube")):
    ydl_opts = {
        "cookiefile": "cookies.txt" if os.path.exists("cookies.txt") else None,
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
    }
    ydl_opts = {k: v for k, v in ydl_opts.items() if v is not None}

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

            stream_url = info.get("url")
            if not stream_url:
                for fmt in info.get("formats", []) or []:
                    if fmt.get("url") and fmt.get("vcodec") in (None, "none"):
                        stream_url = fmt["url"]
                        break

            if not stream_url:
                raise HTTPException(status_code=400, detail="No se encontró un flujo de audio compatible.")

            return {
                "status": "success",
                "meta": {
                    "title": info.get("title"),
                    "artist": info.get("uploader", "Artista Desconocido"),
                    "duration_seconds": info.get("duration", 0),
                    "thumbnail": info.get("thumbnail"),
                },
                "stream_url": stream_url,
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error al extraer datos de YouTube: {str(exc)}")
