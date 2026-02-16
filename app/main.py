import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, HTTPException, Query
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel
import logging

from .fetcher import fetch_and_parse_data
from .cache import cache


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


FETCH_TIME = os.getenv("FETCH_TIME", "18:00")


class ListItem(BaseModel):
    id: int
    lat: float
    lon: float
    type: str


class StatsResponse(BaseModel):
    total: int
    by_provincia: dict
    by_tipo: dict
    by_stato: dict
    by_availability_year: dict
    updated_at: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()

    hour, minute = map(int, FETCH_TIME.split(":"))
    scheduler.add_job(fetch_and_parse_data, "cron", hour=hour, minute=minute)
    scheduler.start()
    logger.info(f"Scheduler started - daily fetch at {FETCH_TIME}")

    await fetch_and_parse_data()

    yield

    scheduler.shutdown()


app = FastAPI(
    title="FiberCop Data API",
    description="Unofficial mirror API for FiberCop's latest CRO/CNO data. Provides endpoints to access raw data, download CSV, get compact lists for map display, retrieve detailed records by ID, and view dataset statistics.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "FiberCop Data API",
        "description": "Unofficial mirror API for FiberCop's latest CRO/CNO data. Provides endpoints to access raw data, download CSV, get compact lists for map display, retrieve detailed records by ID, and view dataset statistics.",
        "endpoints": [
            "/raw",
            "/download",
            "/list",
            "/listmap",
            "/details/{id}",
            "/stats",
            "/health",
            "/docs",
        ],
    }


@app.get("/raw")
async def get_raw_data():
    data = cache.get_data()
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": "public, max-age=86400",
            "ETag": f'"{cache.latest_date.isoformat() if cache.latest_date else "none"}"',
        },
    )


@app.get("/download")
async def download_csv():
    csv_bytes, filename = cache.get_csv()

    if not csv_bytes:
        return JSONResponse(
            content={"error": "No data available"},
            status_code=404,
        )

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "public, max-age=86400",
        },
    )


@app.get(
    "/list",
    response_model=List[ListItem],
    responses={
        200: {
            "description": "List of all records with minimal fields for map display",
            "headers": {
                "Cache-Control": {
                    "description": "Cache control header",
                    "example": "public, max-age=3600",
                },
                "ETag": {
                    "description": "Entity tag for cache validation",
                    "example": '"2026-02-16T18:00:00"',
                },
            },
        }
    },
    summary="Get list of all records",
    description="Returns a compact list of all records with integer ID (id), coordinates (lat, lon), and type. Field names are human-readable for easy consumption. Optimized for map display with minimal data transfer. Uses ultra-fast in-memory lookup by integer ID.",
)
async def get_list():
    data = cache.get_list_data()
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": "public, max-age=86400",
            "ETag": f'"{cache.latest_date.isoformat() if cache.latest_date else "none"}"',
        },
    )


@app.get(
    "/listmap",
    responses={
        200: {
            "description": "Ultra-optimized array of arrays for map rendering",
            "content": {
                "application/json": {
                    "example": [
                        [0, 45.359859, 11.780167, 0],
                        [1, 45.367344, 11.794115, 0],
                        [128, 40.938049, 14.371440, 1],
                    ],
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": [
                                {"type": "integer", "description": "Record ID"},
                                {"type": "number", "description": "Latitude"},
                                {"type": "number", "description": "Longitude"},
                                {
                                    "type": "integer",
                                    "description": "Type: 0=CRO, 1=CNO",
                                },
                            ],
                        },
                    },
                }
            },
            "headers": {
                "Cache-Control": {
                    "description": "Cache control header",
                    "example": "public, max-age=86400",
                },
                "ETag": {
                    "description": "Entity tag for cache validation",
                    "example": '"2026-02-16T18:00:00"',
                },
            },
        }
    },
    summary="Get optimized list for map",
    description="Returns an array-of-arrays format: [[id, lat, lon, type], ...]. Type is encoded as integer: 0=CRO, 1=CNO. Minimal data transfer for maximum performance on map rendering.",
)
async def get_listmap():
    data = cache.get_listmap_data()
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": "public, max-age=86400",
            "ETag": f'"{cache.latest_date.isoformat() if cache.latest_date else "none"}"',
        },
    )


@app.get(
    "/details/{id}",
    responses={
        200: {
            "description": "Full record details",
            "content": {
                "application/json": {
                    "example": {
                        "PROVINCIA": "PADOVA",
                        "COMUNE": "ABANO TERME",
                        "LATITUDINE": "45.359859",
                        "LONGITUDINE": "11.780167",
                        "CODICE_ACL": "04901I",
                        "CENTRALE_TX_DI_RIF": "PADOITAR",
                        "ID_ELEMENTO": "PADOITA002B",
                        "TIPO": "CRO",
                        "TIPOLOGIA_CRO": "STANDARD",
                        "STATO": "DISPONIBILE",
                        "DATA_DISPONIBILITA": "20220725",
                        "INDIRIZZO": "VIA ARMANDO PILLON 15",
                        "DATA_PUBBLICAZIONE": "20220526",
                    }
                }
            },
        },
        404: {
            "description": "Record not found",
            "content": {"application/json": {"example": {"error": "Record not found"}}},
        },
    },
    summary="Get record details by ID",
    description="Retrieve complete record information using integer ID. Returns all CSV fields including address, status, and dates.",
)
async def get_details(id: int):
    record = cache.get_detail_by_id(id)
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return JSONResponse(
        content=record,
        headers={
            "Cache-Control": "public, max-age=86400",
            "ETag": f'"{cache.latest_date.isoformat() if cache.latest_date else "none"}"',
        },
    )


@app.get(
    "/stats",
    response_model=StatsResponse,
    responses={
        200: {
            "description": "Statistics about the dataset",
            "content": {
                "application/json": {
                    "example": {
                        "total": 94330,
                        "by_provincia": {"CATANIA": 487, "MILANO": 54, "OTHER": 93789},
                        "by_tipo": {"CRO": 94200, "CNO": 130},
                        "by_stato": {"DISPONIBILE": 94100, "PROGRAMMATO": 230},
                        "by_availability_year": {
                            "2024": 45000,
                            "2025": 30000,
                            "2023": 15000,
                            "2022": 4000,
                            "2026": 330,
                        },
                        "updated_at": "2026-02-16T18:00:00",
                    }
                }
            },
        }
    },
    summary="Get dataset statistics",
    description="Returns aggregated statistics including total record count, breakdowns by province/type/status, availability year distribution, and last update timestamp. All counts include records with missing fields.",
)
async def get_stats():
    stats = cache.get_stats()
    return JSONResponse(
        content=stats,
        headers={
            "Cache-Control": "public, max-age=86400",
        },
    )


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "fetch_status": cache.fetch_status,
        "last_fetch": cache.last_fetch_time.isoformat()
        if cache.last_fetch_time
        else None,
        "latest_date": cache.latest_date.isoformat() if cache.latest_date else None,
        "record_count": len(cache.parsed_data),
    }
