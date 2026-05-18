#!/usr/bin/env python3
"""
Safe additive-only metadata importer for mastercard_v2 using PokémonTCG.io.

Usage:
  # Preview missing cards without writing anything
  python scripts/import_pokemontcg_metadata.py --dry-run

  # Insert missing metadata rows only
  DATABASE_URL=postgresql+asyncpg://... python scripts/import_pokemontcg_metadata.py

Optional:
  POKEMONTCG_API_KEY=<key>  # Raises PokémonTCG.io rate limits when present.

Safety guarantees:
  - Only INSERT statements are issued; existing mastercard_v2 rows are never updated.
  - Rows are skipped when unique_id already exists.
  - Pricing tables/columns are not read from or written to.
  - PokémonTCG.io card numbers are preserved exactly as returned by the API.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable

import httpx
from dotenv import load_dotenv
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import create_async_engine

API_BASE_URL = "https://api.pokemontcg.io/v2"
PAGE_SIZE = 250
DEFAULT_SINCE_DATE = "2025-03-28"
HTTP_TIMEOUT_SECONDS = 60.0
MAX_HTTP_RETRIES = 3
HTTP_RETRY_BACKOFF_SECONDS = 1.0
DEFAULT_TIER = 3
TARGET_TABLE = "mastercard_v2"

# Keep this list intentionally narrow and metadata-only. Do not add price fields here.
IMPORT_COLUMNS = [
    "unique_id",
    "card_name",
    "set_name",
    "set_id",
    "set_code",
    "card_number",
    "card_number_raw",
    "rarity",
    "artist",
    "type",
    "types",
    "supertype",
    "subtypes",
    "release_date",
    "language",
    "card_image_url",
    "set_logo_url",
    "set_symbol_url",
    "card_slug",
    "set_slug",
    "query",
    "tier",
]


@dataclass
class ImportStats:
    sets_fetched: int = 0
    sets_checked: int = 0
    sets_skipped_by_date: int = 0
    sets_skipped_unparseable_date: int = 0
    cards_checked: int = 0
    inserted: int = 0
    skipped: int = 0
    missing_candidate_count: int = 0
    inserted_set_names: set[str] = field(default_factory=set)
    dry_run_examples: list[dict[str, str | None]] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Additive-only PokémonTCG.io metadata importer for mastercard_v2. "
            "Existing rows are skipped and never updated."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and compare records, but do not insert anything.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=PAGE_SIZE,
        help=f"PokémonTCG.io page size for card requests (default: {PAGE_SIZE}).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of would-insert examples to print during dry-run (default: 10).",
    )
    parser.add_argument(
        "--since-date",
        type=parse_iso_date_arg,
        default=parse_iso_date_arg(DEFAULT_SINCE_DATE),
        help=(
            "Only fetch cards for PokémonTCG.io sets with releaseDate after this "
            f"date, in YYYY-MM-DD format (default: {DEFAULT_SINCE_DATE})."
        ),
    )
    return parser.parse_args()


def normalize_database_url(database_url: str) -> str:
    """Allow common SQLAlchemy PostgreSQL URLs while forcing asyncpg."""
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+asyncpg://", 1)
    return database_url


def slugify(value: str | None) -> str | None:
    if not value:
        return None
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-")
    return slug or None


def parse_iso_date_arg(value: str) -> date:
    parsed = parse_iso_date(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected YYYY-MM-DD"
        )
    return parsed


def parse_iso_date(value: str | None) -> date | None:
    if not value or not isinstance(value, str):
        return None

    normalized = value.strip().replace("/", "-")
    if not normalized:
        return None

    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return None


def filter_sets_after_date(
    sets: Iterable[dict[str, Any]], since_date: date
) -> tuple[list[dict[str, Any]], int, int]:
    filtered_sets: list[dict[str, Any]] = []
    skipped_by_date = 0
    skipped_unparseable = 0

    for pokemon_set in sets:
        release_date = parse_iso_date(pokemon_set.get("releaseDate"))
        if release_date is None:
            skipped_unparseable += 1
            continue
        if release_date <= since_date:
            skipped_by_date += 1
            continue
        filtered_sets.append(pokemon_set)

    return filtered_sets, skipped_by_date, skipped_unparseable


def compact_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def first_image_url(card: dict[str, Any]) -> str | None:
    images = card.get("images") or {}
    return images.get("large") or images.get("small")


def build_import_row(card: dict[str, Any]) -> dict[str, Any]:
    card_set = card.get("set") or {}
    set_images = card_set.get("images") or {}
    card_name = card.get("name")
    set_name = card_set.get("name")
    card_number = card.get("number")
    types = card.get("types") or []
    subtypes = card.get("subtypes") or []

    return {
        "unique_id": card.get("id"),
        "card_name": card_name,
        "set_name": set_name,
        "set_id": card_set.get("id"),
        "set_code": card_set.get("ptcgoCode") or card_set.get("id"),
        "card_number": card_number,
        "card_number_raw": card_number,
        "rarity": card.get("rarity"),
        "artist": card.get("artist"),
        "type": types[0] if types else card.get("supertype"),
        "types": compact_json(types),
        "supertype": card.get("supertype"),
        "subtypes": compact_json(subtypes),
        "release_date": parse_iso_date(card_set.get("releaseDate")),
        "language": "en",
        "card_image_url": first_image_url(card),
        "set_logo_url": set_images.get("logo"),
        "set_symbol_url": set_images.get("symbol"),
        "card_slug": slugify(card_name),
        "set_slug": slugify(set_name),
        "query": " ".join(part for part in [card_name, set_name, card_number] if part),
        "tier": DEFAULT_TIER,
    }


class PokemonTCGFetchError(RuntimeError):
    """Raised when a PokémonTCG.io endpoint cannot be fetched after retries."""


async def fetch_page_with_retries(
    client: httpx.AsyncClient,
    endpoint: str,
    *,
    params: dict[str, Any],
    page: int,
) -> dict[str, Any]:
    request_params = {**params, "page": page}
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            response = await client.get(
                f"{API_BASE_URL}/{endpoint}",
                params=request_params,
            )
            response.raise_for_status()
            return response.json()
        except httpx.ReadTimeout as exc:
            if attempt >= MAX_HTTP_RETRIES:
                raise PokemonTCGFetchError(
                    "Timed out fetching PokémonTCG.io "
                    f"endpoint={endpoint!r} page={page} after "
                    f"{MAX_HTTP_RETRIES} attempts."
                ) from exc

            backoff = HTTP_RETRY_BACKOFF_SECONDS * attempt
            print(
                "⚠️ Read timeout fetching PokémonTCG.io "
                f"endpoint={endpoint!r} page={page}; retrying "
                f"{attempt + 1}/{MAX_HTTP_RETRIES} after {backoff:.1f}s."
            )
            await asyncio.sleep(backoff)

    raise PokemonTCGFetchError(
        f"Unable to fetch PokémonTCG.io endpoint={endpoint!r} page={page}."
    )


async def fetch_all_pages(
    client: httpx.AsyncClient,
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    continue_on_timeout: bool = False,
) -> list[dict[str, Any]]:
    params = dict(params or {})
    params.setdefault("pageSize", PAGE_SIZE)
    page = 1
    all_items: list[dict[str, Any]] = []

    while True:
        try:
            payload = await fetch_page_with_retries(
                client,
                endpoint,
                params=params,
                page=page,
            )
        except PokemonTCGFetchError as exc:
            if not continue_on_timeout:
                raise
            print(f"⚠️ {exc} Continuing with the next set.")
            break

        items = payload.get("data") or []
        all_items.extend(items)

        total_count = payload.get("totalCount")
        if not items or (total_count is not None and len(all_items) >= int(total_count)):
            break
        page += 1

    return all_items


async def get_existing_ids(conn) -> set[str]:
    result = await conn.execute(text(f"SELECT unique_id FROM {TARGET_TABLE}"))
    unique_ids: set[str] = set()
    for (unique_id,) in result.fetchall():
        if unique_id:
            unique_ids.add(str(unique_id).strip())
    return unique_ids


async def get_column_types(conn) -> dict[str, str]:
    result = await conn.execute(
        text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :table_name
            """
        ),
        {"table_name": TARGET_TABLE},
    )
    return {row[0]: row[1] for row in result.fetchall()}


def filter_available_columns(row: dict[str, Any], column_types: dict[str, str]) -> dict[str, Any]:
    return {column: row[column] for column in IMPORT_COLUMNS if column in column_types}


def missing_columns(column_types: dict[str, str]) -> list[str]:
    return [column for column in IMPORT_COLUMNS if column not in column_types]


def should_skip(row: dict[str, Any], existing_unique_ids: set[str]) -> bool:
    unique_id = str(row["unique_id"]).strip() if row.get("unique_id") else None
    return not unique_id or unique_id in existing_unique_ids


def sql_value_expression(column: str, data_type: str) -> str:
    if data_type in {"json", "jsonb"}:
        return f"CAST(:{column} AS {data_type.upper()})"
    if data_type == "date":
        return f"CAST(:{column} AS DATE)"
    return f":{column}"


def build_insert_statement(columns: Iterable[str], column_types: dict[str, str]):
    column_list = list(columns)
    quoted_columns = ", ".join(f'"{column}"' for column in column_list)
    values = ", ".join(sql_value_expression(column, column_types[column]) for column in column_list)
    where_clause = f"NOT EXISTS (SELECT 1 FROM {TARGET_TABLE} WHERE unique_id = :unique_id)"

    stmt = text(
        f"""
        INSERT INTO {TARGET_TABLE} ({quoted_columns})
        SELECT {values}
        WHERE {where_clause}
        """
    )
    for column in column_list:
        stmt = stmt.bindparams(bindparam(column))
    return stmt


async def insert_missing_rows(conn, rows: list[dict[str, Any]], column_types: dict[str, str]) -> int:
    if not rows:
        return 0

    stmt = build_insert_statement(rows[0].keys(), column_types)
    inserted = 0
    for row in rows:
        result = await conn.execute(stmt, row)
        inserted += result.rowcount or 0
    return inserted


async def main() -> None:
    args = parse_args()
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set in environment")

    api_key = os.getenv("POKEMONTCG_API_KEY")
    headers = {"X-Api-Key": api_key} if api_key else {}
    engine = create_async_engine(normalize_database_url(database_url), echo=False)
    stats = ImportStats()

    async with httpx.AsyncClient(
        headers=headers,
        timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS),
    ) as client:
        sets = await fetch_all_pages(
            client,
            "sets",
            params={
                "orderBy": "releaseDate",
                "select": "id,name,ptcgoCode,releaseDate,images",
            },
        )
        stats.sets_fetched = len(sets)
        sets, stats.sets_skipped_by_date, stats.sets_skipped_unparseable_date = (
            filter_sets_after_date(sets, args.since_date)
        )
        stats.sets_checked = len(sets)
        set_names_by_id = {pokemon_set.get("id"): pokemon_set.get("name") for pokemon_set in sets}

        async with engine.begin() as conn:
            column_types = await get_column_types(conn)
            unavailable = missing_columns(column_types)
            if unavailable:
                print(
                    "⚠️ mastercard_v2 is missing these requested metadata columns; "
                    f"they will be skipped: {', '.join(unavailable)}"
                )

            existing_unique_ids = await get_existing_ids(conn)
            rows_to_insert: list[dict[str, Any]] = []
            cards_by_missing_set: dict[str, int] = {}

            for pokemon_set in sets:
                set_id = pokemon_set.get("id")
                if not set_id:
                    continue

                cards = await fetch_all_pages(
                    client,
                    "cards",
                    params={
                        "pageSize": args.page_size,
                        "q": f"set.id:{set_id}",
                        "orderBy": "set.releaseDate,set.id,number",
                        "select": (
                            "id,name,number,rarity,artist,types,supertype,"
                            "subtypes,images,set"
                        ),
                    },
                    continue_on_timeout=True,
                )

                for card in cards:
                    stats.cards_checked += 1
                    row = build_import_row(card)
                    if should_skip(row, existing_unique_ids):
                        stats.skipped += 1
                        continue

                    stats.missing_candidate_count += 1
                    set_name = row.get("set_name") or set_names_by_id.get(set_id) or set_id
                    cards_by_missing_set[set_name] = cards_by_missing_set.get(set_name, 0) + 1
                    if len(stats.dry_run_examples) < args.sample_size:
                        stats.dry_run_examples.append(
                            {
                                "unique_id": row.get("unique_id"),
                                "card_name": row.get("card_name"),
                                "set_name": row.get("set_name"),
                                "card_number": row.get("card_number"),
                            }
                        )
                    rows_to_insert.append(filter_available_columns(row, column_types))

            if not args.dry_run:
                stats.inserted = await insert_missing_rows(conn, rows_to_insert, column_types)
                stats.skipped += stats.missing_candidate_count - stats.inserted
                stats.inserted_set_names = set(cards_by_missing_set) if stats.inserted else set()

    await engine.dispose()

    if args.dry_run:
        stats.inserted_set_names = set(cards_by_missing_set)
        print("🧪 DRY RUN ONLY — no rows inserted.")
        print(f"Would insert: {stats.missing_candidate_count}")
        if stats.dry_run_examples:
            print("Sample would-insert rows:")
            for example in stats.dry_run_examples:
                print(
                    "  - "
                    f"{example['unique_id']} | {example['card_name']} | "
                    f"{example['set_name']} #{example['card_number']}"
                )
    else:
        print("✅ Import complete.")

    print(f"Since date: {args.since_date.isoformat()} (exclusive)")
    print(f"Sets fetched: {stats.sets_fetched}")
    print(f"Sets skipped by date: {stats.sets_skipped_by_date}")
    print(
        "Sets skipped with unparseable releaseDate: "
        f"{stats.sets_skipped_unparseable_date}"
    )
    print(f"Sets checked: {stats.sets_checked}")
    print(f"Cards checked: {stats.cards_checked}")
    print(f"Inserted: {stats.inserted if not args.dry_run else 0}")
    print(f"Skipped: {stats.skipped}")
    print("Inserted set names:")
    if stats.inserted_set_names:
        for set_name in sorted(stats.inserted_set_names):
            count_suffix = ""
            if args.dry_run:
                count_suffix = f" ({cards_by_missing_set.get(set_name, 0)} cards)"
            print(f"  - {set_name}{count_suffix}")
    else:
        print("  - None")


if __name__ == "__main__":
    asyncio.run(main())
