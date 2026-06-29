FROM python:3.10-slim

# Instalar FFmpeg nativo y dependencias para compilar curl_cffi con BoringSSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libcurl4-openssl-dev \
    libssl-dev \
    && rm -rf /lib/apt/lists/*

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

# Forzar la actualización limpia eliminando la memoria caché de pip
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]