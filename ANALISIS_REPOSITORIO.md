# Análisis técnico del repositorio `vibetune-backend`

Fecha de análisis: 2026-06-29

## Resumen ejecutivo

El repositorio contiene una API FastAPI en un único archivo (`main.py`) para convertir enlaces de YouTube en audio, almacenar el resultado en Supabase Storage y consultar el estado de procesamiento. La base funcional existe, pero el proyecto concentra demasiadas responsabilidades en un solo módulo y tenía un problema crítico de seguridad: un archivo de cookies de YouTube con credenciales de sesión estaba versionado en `auth/cookies.txt`.

En esta revisión corregí el riesgo más urgente:

- Eliminé `auth/cookies.txt` del repositorio.
- Añadí `.gitignore` para evitar volver a versionar secretos, entornos virtuales y artefactos locales.
- Ajusté `main.py` para que `yt-dlp` use cookies solo si `YT_COOKIES_FILE` apunta a un archivo existente, manteniendo el servicio funcional sin depender de un secreto embebido.

> Importante: aunque el archivo fue eliminado del estado actual del repositorio, si ya fue subido a un remoto, las cookies deben considerarse comprometidas. Se recomienda cerrar sesiones/revocar cookies de la cuenta afectada y rotar cualquier secreto relacionado.

## Estructura actual

```text
.
├── Dockerfile
├── main.py
├── requirements.txt
├── .gitignore
└── ANALISIS_REPOSITORIO.md
```

## Hallazgos principales

### 1. Seguridad: cookies sensibles versionadas

**Severidad:** crítica

El repositorio incluía un archivo Netscape de cookies en `auth/cookies.txt`. Este tipo de archivo puede permitir acceso autenticado a servicios de terceros mientras las cookies sigan vigentes.

**Corrección aplicada:**

- Se eliminó `auth/cookies.txt`.
- Se añadió `auth/cookies.txt` a `.gitignore`.
- Se modificó la configuración de `yt-dlp` para no asumir que el archivo existe.

**Recomendación adicional:**

- Revocar/cerrar la sesión asociada a esas cookies.
- Si el repositorio ya fue publicado, limpiar el historial con herramientas como `git filter-repo` o rotar completamente las credenciales expuestas.
- Montar cookies mediante secretos del proveedor de despliegue, por ejemplo como archivo temporal o volumen seguro.

### 2. Configuración rígida al importar `main.py`

**Severidad:** media

`main.py` valida `SUPABASE_URL` y `SUPABASE_SERVICE_KEY` durante el import. Esto simplifica el arranque en producción, pero dificulta pruebas unitarias, linters y análisis estático en entornos sin variables reales.

**Mejor solución propuesta:**

- Crear un módulo `settings.py` con `pydantic-settings`.
- Construir clientes externos durante el lifespan de FastAPI, no como efecto secundario global.
- Permitir un modo de test con dependencias inyectables.

Ejemplo deseado:

```python
class Settings(BaseSettings):
    supabase_url: AnyUrl
    supabase_service_key: SecretStr
    bucket_name: str = "vibetune-tracks"
    max_duration_seconds: int = 7200
```

### 3. `main.py` concentra demasiadas responsabilidades

**Severidad:** media

El archivo principal contiene configuración, seguridad, rate limiting, limpieza de metadata, extracción de YouTube, almacenamiento, endpoints, métricas y ciclo de vida.

**Mejor solución propuesta:**

Dividir por dominios:

```text
app/
├── main.py
├── core/
│   ├── config.py
│   ├── logging.py
│   └── metrics.py
├── api/
│   └── routes.py
├── services/
│   ├── youtube.py
│   ├── metadata_cleaner.py
│   ├── storage.py
│   └── tracks.py
└── models/
    └── tracks.py
```

Beneficios:

- Pruebas más simples.
- Menos acoplamiento.
- Menor riesgo al modificar extracción, storage o endpoints.
- Mejor mantenibilidad.

### 4. Rate limiting en memoria

**Severidad:** media

El rate limiting actual usa un diccionario global en memoria. Funciona en una sola instancia, pero no protege correctamente si la API escala horizontalmente o si se reinicia el proceso.

**Mejor solución propuesta:**

- Usar Redis o Upstash para rate limiting distribuido.
- Definir límites por IP, endpoint y eventualmente usuario/API key.
- Registrar métricas de rechazos por endpoint.

### 5. Trabajo de descarga en tareas internas de FastAPI

**Severidad:** media/alta según carga

La descarga y transcodificación se ejecutan como tareas asíncronas dentro del proceso web. Esto puede ser suficiente para bajo tráfico, pero tiene riesgos:

- Si el contenedor se reinicia, se interrumpen trabajos.
- El proceso web compite con FFmpeg por CPU y memoria.
- Es difícil reintentar trabajos de forma controlada.

**Mejor solución propuesta:**

- Usar una cola de trabajos: Celery, RQ, Dramatiq, arq o un worker propio con Redis/Postgres.
- Mantener la API solo como orquestador.
- Persistir intentos, errores y tiempos de ejecución.

### 6. Dependencia de espejos públicos

**Severidad:** media

La extracción de metadata recurre a espejos públicos de Invidious/Piped cuando falla `yt-dlp`. Esto mejora resiliencia, pero introduce dependencia en terceros no controlados.

**Recomendaciones:**

- Hacer la lista configurable por variable de entorno.
- Añadir timeouts por espejo y métrica por proveedor.
- No confiar en datos de espejos para decisiones críticas sin validación.

### 7. Dockerfile mejorable

**Severidad:** baja/media

El Dockerfile instala dependencias en una imagen `python:3.10-slim`, pero puede endurecerse.

**Mejoras sugeridas:**

- Ejecutar como usuario no root.
- Añadir `PYTHONUNBUFFERED=1` y `PYTHONDONTWRITEBYTECODE=1`.
- Considerar healthcheck.
- Revisar el comentario de `rm -rf /lib/apt/lists/*`: normalmente se limpia `/var/lib/apt/lists/*`.
- Fijar versiones críticas si se requiere reproducibilidad estricta.

### 8. Observabilidad sólida pero con mejoras posibles

**Fortalezas:**

- Usa `structlog`.
- Expone `/metrics` con Prometheus.
- Registra tiempos de request y errores de `yt-dlp`.

**Mejoras sugeridas:**

- Usar labels de endpoint normalizados en métricas para evitar alta cardinalidad.
- Añadir métricas de duración de descarga, tamaño de archivo y estado final del job.
- Propagar `trace_id` al worker de descarga.

## Correcciones aplicadas en esta rama

### `.gitignore`

Se agregó un `.gitignore` con exclusiones para:

- Cachés de Python.
- Entornos virtuales.
- Archivos `.env`.
- Cookies locales en `auth/cookies.txt`.
- Claves privadas y logs.

### `main.py`

Se cambió la configuración de `YDL_OPTS` para que:

- No incluya una ruta hardcodeada a cookies.
- Use `YT_COOKIES_FILE` solo si el archivo existe.
- Emita un warning estructurado si no hay cookies configuradas.

### `auth/cookies.txt`

Se eliminó del repositorio por contener material sensible.

## Prioridades recomendadas

### Corto plazo

1. Rotar/revocar cookies expuestas.
2. Configurar secretos en el entorno de despliegue.
3. Añadir pruebas unitarias para:
   - `canonicalize_youtube_url`.
   - `MetadataCleaner.clean`.
   - `detect_content_type`.
4. Añadir CI con `ruff`, `mypy` o `pyright`, y `pytest`.

### Mediano plazo

1. Separar `main.py` en módulos.
2. Introducir `settings.py` con validación de configuración.
3. Sustituir rate limiting en memoria por Redis.
4. Normalizar métricas para evitar alta cardinalidad.

### Largo plazo

1. Mover descargas/transcodificación a workers externos.
2. Añadir reintentos con backoff y dead-letter queue.
3. Añadir limpieza de archivos huérfanos y reconciliación periódica con Supabase.
4. Definir un contrato OpenAPI estable para clientes externos.

## Comandos útiles para validar localmente

```bash
python -m py_compile main.py
```

```bash
SUPABASE_URL="https://example.supabase.co" SUPABASE_SERVICE_KEY="dummy" python -c "import main; print(main.YDL_OPTS)"
```

> Nota: importar `main.py` con credenciales falsas puede intentar inicializar el cliente de Supabase. Para pruebas reales conviene refactorizar configuración e inyección de dependencias.
