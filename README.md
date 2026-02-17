# FiberCop Data API
Unofficial mirror API for FiberCop's latest CRO/CNO data. Provides endpoints to access raw data, download CSV, get compact lists for map display, retrieve detailed records by ID, and view dataset statistics.

*Public API at*: https://fbdapi.vitto.dev/
*Documentation*: https://fbdapi.vitto.dev/docs

## Some technical info

**Stack**: FastAPI + Uvicorn (async Python), Docker

**Architecture**:
- **fetcher.py**: Daily scheduled job (APScheduler) downloads FiberCop's ZIP, extracts CSV with 90k+ CRO/CNO records
- **cache.py**: Thread-safe singleton cache with pre-computed views (listmap for maps, details dict for O(1) ID lookups, stats aggregations)
- **main.py**: FastAPI with 7 endpoints - `/raw`, `/list` (objects), `/listmap` (arrays for map perf), `/download` (CSV), `/details/{id}`, `/stats`, `/health`

**Some optimizations**:
- Integer-based IDs (not UUIDs or already existing fields) for faster lookup
- Array-of-arrays format `[[id,lat,lon,type],...]` minimizes JSON payload
- ETag + 24h cache headers on all responses
- Disk-persisted CSV survives container restart
