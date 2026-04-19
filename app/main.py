from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "orders.db"
USERS_SERVICE_URL = os.getenv("USERS_SERVICE_URL", "http://127.0.0.1:8001")
HTTP_CLIENT = httpx.AsyncClient(timeout=10.0, trust_env=False)


class OrderCreate(BaseModel):
    user_id: int
    item_name: str = Field(min_length=2, max_length=100)
    quantity: int = Field(ge=1, le=100)


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.commit()


async def ensure_user_exists(user_id: int) -> None:
    try:
        response = await HTTP_CLIENT.get(f"{USERS_SERVICE_URL}/users/{user_id}")
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"Users service is unavailable: {error}") from error

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found in users_service")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Unexpected users_service error: {response.text}")


init_db()
app = FastAPI(title="Orders Service", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "orders_service",
        "users_service_url": USERS_SERVICE_URL,
    }


@app.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_order(payload: OrderCreate) -> dict:
    await ensure_user_exists(payload.user_id)

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO orders(user_id, item_name, quantity, status)
            VALUES (?, ?, ?, ?)
            """,
            (payload.user_id, payload.item_name, payload.quantity, "created"),
        )
        connection.commit()
        order_id = cursor.lastrowid
        row = connection.execute(
            """
            SELECT id, user_id, item_name, quantity, status, created_at
            FROM orders
            WHERE id = ?
            """,
            (order_id,),
        ).fetchone()
    return dict(row)


@app.get("/orders")
def list_orders() -> list[dict]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, user_id, item_name, quantity, status, created_at
            FROM orders
            ORDER BY id
            """
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/orders/{order_id}")
def get_order(order_id: int) -> dict:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, user_id, item_name, quantity, status, created_at
            FROM orders
            WHERE id = ?
            """,
            (order_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return dict(row)
