# CardCatch Data/Pricing Pipeline Audit

Date: 2026-05-08
Scope: repository audit as a data/pricing pipeline asset. This report intentionally does **not** modify application code.

## Executive summary

CardCatch is a hybrid pricing pipeline for Pokémon cards. It combines a spreadsheet-era master workbook, JSON/CSV batch manifests, ad-hoc Python scripts, FastAPI endpoints, SQLModel table definitions, and direct SQL scripts that write to production-like PostgreSQL tables.

The current asset is useful as a prototype and operational toolkit, but it should not remain JSON/spreadsheet-driven for production pricing. The core issue is not JSON itself; it is that job state, card identity, raw observations, cleaned aggregates, null results, failures, and tier scheduling are spread across files and loosely coupled tables with inconsistent identifiers and implicit semantics.

The recommended V2 direction is:

1. Keep JSON only as import/export and debug artifacts.
2. Make PostgreSQL the system of record.
3. Split the model into four pricing pipeline layers:
   - canonical `cards`
   - `pricing_jobs` / job items
   - immutable `raw_price_observations`
   - versioned `cleaned_price_snapshots`
4. Move tiering from hard-coded scripts into a policy-backed schedule table or deterministic computed fields.
5. Preserve every raw marketplace observation and every cleaning decision with provenance, run ID, parser version, filters applied, and confidence.

## Repository inventory: what each file is for

| Path | Purpose | Notes / concerns |
|---|---|---|
| `CardBrain_Master.xlsx` | Legacy spreadsheet source of truth and operational workbook. Contains sheets for hot characters, set sizes, master card library, daily price logs, active price logs, tier tabs, inventory, wishlist, trend tracking, smart suggestions, bundles, and dashboard-style outputs. | The workbook mixes canonical data, mutable pricing snapshots, derived analytics, manual workflow, and presentation. It should be treated as a legacy import/export artifact, not the production database. |
| `Data/Scraper_Batch_Input.csv` | CSV manifest of 149 numeric `unique_id` values and eBay query strings for scraper batches. | Uses old numeric IDs, while newer data uses Pokémon TCG API-style IDs such as `sm7-154`. |
| `cards_due.json` | Current file-based queue/manifest of cards due for scraping. Contains `unique_id`, `query`, and `tier`; currently 2,086 entries, all tier 5 in this checkout. | This is operational job state and should move into `pricing_jobs` and `pricing_job_items`. |
| `requirements.txt` | Python dependency list for FastAPI app and scripts. | Missing `openpyxl` despite Excel ingestion scripts using `pandas.read_excel`. Duplicates `python-dotenv`. |
| `render.yaml` | Render deployment definition for the FastAPI service. | Starts `uvicorn api.main:app` on port 10000. |
| `api/main.py` | FastAPI service. Provides health endpoint, bulk master-card upsert endpoint, scraped price endpoints, async TCG batch price endpoint, and scrape launcher endpoint. | Imports `BatchManager`/launcher and `MasterCard`; scraped-price routes reference scraper functions that are not imported in this file. |
| `models/models.py` | SQLModel declarations for `mastercard_v2`, legacy `mastercard`, `dailypricelog`, `activedailypricelog`, `trendtracker`, `smartsuggestions`, `inventory`, and `wishlist`. | Models do not cover all SQL tables used by scripts; several fields used by raw SQL are absent from models. |
| `utils.py` | Shared scraper utility functions: exclusion terms, outlier filtering, median/average helpers, listing title validation, holo-type detection, and query metadata parsing. | Query parsing assumes the first token is the character and a `number/total` pattern exists, which is not true for every query format. |
| `batch_manager.py` | Produces batches of legacy `MasterCard` rows ordered by tier for TCG, sold, or active scrapers. | Uses legacy `mastercard` model and only tiers `1`-`4` plus blank, while V2 tiering uses numeric 1-9 in other scripts. |
| `scraper_controller.py` | Main orchestration script. Determines due cards by tier intervals, writes `cards_due.json`, runs the dual eBay scraper, runs TCG updater, then runs clean/tier recalculation. | Uses JSON as a queue; tier intervals only defined for tiers 1-4, even though V2 tiering assigns 1-9. |
| `archive/scraper.py` | Core eBay HTML parser for sold and active listings. Builds eBay URLs, parses raw listings, applies utility filters, returns raw and filtered observations. | Despite living in `archive`, it is actively imported by current scripts. |
| `archive/scrape_ebay_dual.py` | Current dual eBay scraper driven by `cards_due.json`. Scrapes sold and active listings, logs raw/debug rows, inserts daily sold and active aggregates. | Persists both raw observations and aggregate rows, but without a formal job/run ID. |
| `archive/scraper_launcher.py` | Placeholder async launcher that gets batches and simulates TCG/sold/active scraping. | References `card.name`, but model fields are `card_name`; appears non-production/stub. |
| `archive/upload_master_cards.py` | Legacy one-time Excel-to-DB uploader for the `mastercard` table. | Uses Excel workbook as source of truth and legacy numeric `MasterCard` schema. |
| `upload/upload_master_cards_api.py` | Uploads `CardBrain_Master.xlsx` master card rows to the live API `/bulk-upsert-master-cards`. | Depends on `openpyxl` transitively but requirements do not include it. |
| `ebay_sold_scraper.py` | Standalone sold-listings scraper over `mastercard_v2`, with URL tracking, null logging, failure logging, and `dailypricelog` inserts. | Assumes parser returns an iterable of listing dicts, but `archive.scraper.parse_ebay_sold_page` returns `{url, raw, filtered}`. |
| `null_rescrape.py` | Retries cards from `ebay_sold_nulls`, re-scrapes sold listings, logs retry nulls and daily price logs. | Similar logic to `ebay_sold_scraper.py`; should become a job retry mode rather than a separate path. |
| `tcg_price_updater.py` | Pulls card IDs from `cards_due.json` or `mastercard_v2`, calls the FastAPI TCG batch endpoint, and inserts `tcg_pricing_log` rows. | Normalizes card IDs to uppercase for API calls, while other tables use lowercase IDs. |
| `populate_mastercard_v2.py` | Aggregates sold, TCG, and active logs into price columns on `mastercard_v2`. | Name implies initial population, but function is price roll-up. Uses `execute_many` on `AsyncSession`, which is not a standard SQLAlchemy async API method. |
| `update_mastercard_prices.py` | Updates `mastercard_v2.active_ebay_median` from historical `activedailypricelog` medians. | Contains a hard-coded PostgreSQL connection string. |
| `update_clean_and_tiers.py` | Post-scrape roll-up: computes `clean_avg_value`, verified sale counts, price min/max, and tier from sold/active/TCG data plus wishlist/inventory/hot-character flags. | Contains the clearest implementation of tier semantics, but also a hard-coded database credential. |
| `historical_pricelog_cleanse.py` | Audits `dailypricelog` rows and flags suspicious rows as `trusted = FALSE` based on sample count, spread, and high-price heuristics. | Mutates production logs and has a hard-coded database credential. |
| `analysis/check_unlogged_cards.py` | Scrapes cards listed in `./data/unlogged_cards.txt` that have no sales data, inserting daily price log rows or null-price rows. | Imports `models`, `scraper` paths inconsistently relative to repo layout. |
| `analysis/generate_trend_tracker.py` | Rebuilds `trendtracker` from 30-day daily sold-price history, excluding tier 4 legacy cards, computing last/second/third prices and trend tags. | Joins `dailypricelog` to legacy `mastercard`, not `mastercard_v2`. |
| `analysis/generate_smart_suggestions.py` | Builds buy/sell/bundle/watch suggestions from `TrendTracker` and legacy `MasterCard` clean/resale values. | Uses legacy model imports and hard-coded hot-character list. |
| `analysis/backfill_affiliate_links.py` | Backfills affiliate eBay search links onto `SmartSuggestion` records using `MasterCard.query`. | Uses legacy `MasterCard`, not `mastercard_v2`. |
| `parse_expansions_local.py` | Parses a local `expansions.html` file into set-like rows in `mastercard_v2`. | Source file is not present in repo; also stores set rows in a card table with synthetic `SET_*` IDs. |
| `test_connection.py` | Simple database connectivity smoke test using `SELECT NOW()`. | Uses SQLAlchemy sync engine against `DATABASE_URL`; async URLs may need conversion. |

## How card IDs map to search queries

There are two identity systems in the repository.

### 1. Legacy numeric `unique_id`

The older workbook/API path uses integer `Unique ID` values and stores the Pokémon TCG API ID separately as `Card ID`. The query is stored as `Full Query` and generally concatenates:

```text
<Card Name> <Set Name> <Card Number>
```

Examples from the workbook and CSV include:

```text
Unique ID: 2871
query: M Blastoise-EX Generations 18/083
```

This means the numeric ID is an internal surrogate key, not derivable from the query. The searchable card identity is the full query string plus the separate card metadata columns.

### 2. V2 Pokémon TCG-style `unique_id`

The newer `mastercard_v2` and `cards_due.json` path uses IDs such as:

```text
sm7-154 -> Articuno GX Celestial Storm 154
xy12-1  -> Venusaur EX Evolutions 1
```

This appears to match the Pokémon TCG API card ID convention:

```text
<set_code>-<card_number_without_leading_zeroes>
```

In this checkout, `cards_due.json` contains 2,086 cards. Of those, 1,624 match the simple pattern `^[a-z0-9]+-\d+$`, and every one of those 1,624 has a numeric suffix that matches the final numeric token in the query. The remaining 462 have non-simple IDs, likely IDs with alternate numbering, promos, suffixes, or forms that do not fit the simple regex.

### Query construction assumptions currently embedded in code

Current scrapers assume the query can be parsed by position:

- The first token is the character/card-name anchor.
- A card number can be extracted as either `\d+/\d+` or by taking digits after the first token.
- The eBay title must contain the character anchor and the numeric card identifier.
- Exclusion keywords remove graded cards, lots, bundles, proxies, damaged cards, language variants, first editions, etc.

This works for many English raw-card searches, but it is brittle for:

- multi-word Pokémon names or trainer names;
- names with punctuation, symbols, or variants (`M Blastoise-EX`, `Moltres  Zapdos  Articuno GX`);
- promo cards with blank or unusual card numbers;
- set names that include meaningful numbers;
- card names where the first token is not a unique title anchor;
- queries with `number/total` versus queries with only a short number.

## What the tier system likely means

The tier system is a scrape priority and commercial-action priority, not a collectible rarity tier.

The clearest current rule is in `update_clean_and_tiers.py`:

| Tier | Likely meaning | Rule inferred from current code |
|---:|---|---|
| 1 | Highest operational priority | Card is in wishlist or inventory. |
| 2 | Hot, mid-value card | Hot character and clean value between £7 and £11 inclusive. |
| 3 | Non-hot, mid-value card | Not hot and clean value between £7 and £11 inclusive. |
| 4 | Hot, high-value card | Hot character and clean value above £11. |
| 5 | Non-hot, high-value card | Not hot and clean value above £11. |
| 6 | Hot, lower-mid card | Hot character and clean value from £3 to less than £7. |
| 7 | Non-hot, lower-mid card | Not hot and clean value from £3 to less than £7. |
| 8 | Hot, low-value card | Hot character and clean value below £3. |
| 9 | Non-hot, low-value card | Not hot and clean value below £3. |

The controller’s schedule is inconsistent with that nine-tier assignment. It only defines scrape intervals for tiers 1-4:

| Controller tier | Interval |
|---:|---|
| 1 | 1 day |
| 2 | 2 days |
| 3 | 3 days |
| 4 | 7 days |

Because `cards_due.json` currently contains only tier 5 rows, that file is probably a manual override or force-all output rather than the controller’s natural due-card output. Under the current controller policy, tier 5 cards would not be selected by the due-card scheduler unless manually forced or placed into JSON.

Recommended interpretation for V2: split “tier” into separate concepts:

- `priority_score`: numeric score used for scheduling.
- `price_band`: low / lower_mid / mid / high.
- `demand_segment`: hot / normal / wishlist / inventory.
- `scrape_interval_hours`: computed or policy-driven.
- `action_segment`: buy, watch, inventory, bundle, ignore.

## Should this remain JSON-based?

No, not as the operational source of truth.

JSON is acceptable for:

- local debug snapshots;
- importing/exporting card manifests;
- replaying a single scraper run;
- archiving an exact request payload;
- storing source-specific raw payload fragments in JSONB columns.

JSON is not appropriate as the primary mechanism for:

- deciding which cards are due;
- tracking job status and retries;
- deduplicating observations;
- enforcing uniqueness and referential integrity;
- calculating freshness windows;
- monitoring failures/null results;
- coordinating concurrent workers.

The current `cards_due.json` role should migrate into database-backed job tables. A job table gives atomic status transitions, retry counts, worker locking, timestamps, and auditability. It also avoids stale files, accidental overwrites, mixed ID formats, and invisible manual edits.

## How to migrate into real database tables

### Phase 1: stabilize identity

1. Treat `mastercard_v2.unique_id` as the canonical external card ID where it is a Pokémon TCG API ID.
2. Add an internal UUID or bigint primary key (`card_pk`) and keep external IDs as unique natural keys.
3. Preserve legacy numeric IDs in `legacy_unique_id` for workbook migration.
4. Normalize card number into both display and sortable components:
   - `card_number_display`, e.g. `001/102`
   - `card_number_raw`, e.g. `1`
   - `card_number_sort`, integer where possible
   - `set_printed_total`, integer where known

### Phase 2: import current artifacts

1. Import `CardBrain_Master.xlsx` `Master Card Library` as staging rows.
2. Import `cards_due.json` as a historical `pricing_jobs` run with one item per card.
3. Import existing `dailypricelog`, `activedailypricelog`, `raw_ebay_*`, and `tcg_pricing_log` into raw observations and cleaned snapshots.
4. Store original query text used for every historical scrape.
5. Backfill `card_id`, `source_card_id`, and `search_query_id` mappings.

### Phase 3: replace file queues

1. Scheduler inserts due cards into `pricing_jobs` and `pricing_job_items`.
2. Workers claim rows with status `queued` using `FOR UPDATE SKIP LOCKED`.
3. Each worker writes raw observations with `job_item_id`.
4. Cleaning jobs read raw observations and write cleaned snapshots.
5. Rollups update card-level latest summary columns or a materialized view.

### Phase 4: deprecate workbook and ad-hoc scripts

1. Workbook becomes an export/report, not an input.
2. Hard-coded credentials are removed; all scripts use environment variables/secrets.
3. One shared scraper library replaces duplicated sold/active/null-rescrape logic.
4. SQLModel/Alembic migrations define every production table and column.

## Risks in the current data structure

### High-risk issues

1. **Hard-coded database credentials** exist in multiple scripts. This is the highest priority remediation item.
2. **Mixed identity systems** (`int` legacy IDs, lowercase TCG IDs, uppercase normalized IDs) create join and deduplication risk.
3. **File-based job state** (`cards_due.json`) is not safe for concurrent workers, retries, or audit trails.
4. **Source-of-truth ambiguity** among Excel, JSON, SQLModel models, raw SQL tables, and API upserts.
5. **Model/schema drift**: scripts use columns and tables not represented in `models/models.py`, including `trusted`, `urls_used`, raw debug tables, failure tables, log tables, and TCG logs.
6. **No formal run/job lineage**: aggregate price rows cannot reliably be traced to a scraper run, parser version, cleaning policy, or source payload.
7. **Raw and cleaned data are mixed**: some tables store raw rows, some daily aggregates, and some latest rollups without a consistent layer boundary.
8. **Brittle query parsing and title matching** can silently exclude valid cards or include wrong cards.
9. **Tier policy inconsistency** between nine-tier assignment and four-tier scheduling can leave tiers 5-9 unscheduled.
10. **Duplicate observations** are likely because inserts do not enforce source URL/item/date uniqueness.

### Medium-risk issues

1. eBay scraping relies on HTML selectors that may break without structured error classification.
2. Currency, shipping, location, condition, language, grading, and quantity are not normalized enough for pricing quality.
3. Active listings and sold listings use `sale_count` naming inconsistently.
4. TCG pricing logs do not clearly distinguish product type/finish (`holofoil`, `normal`, `reverseHolofoil`, etc.).
5. Null results are tracked in side tables instead of first-class job outcomes.
6. Some scripts import modules through inconsistent paths, making deployment/runtime behavior fragile.
7. Excel ingestion needs packages absent from `requirements.txt`.
8. There is no database migration framework in the repo.

## Recommended V2 schema

The schema below is intentionally normalized around pricing-pipeline lineage. Names are suggestions.

### `cards`

Canonical card dimension.

```sql
CREATE TABLE cards (
  card_pk BIGSERIAL PRIMARY KEY,
  external_card_id TEXT UNIQUE NOT NULL,        -- e.g. sm7-154
  legacy_unique_id BIGINT UNIQUE,
  card_name TEXT NOT NULL,
  set_name TEXT NOT NULL,
  set_code TEXT,
  set_id TEXT,
  card_number_display TEXT,
  card_number_raw TEXT,
  card_number_sort INTEGER,
  set_printed_total INTEGER,
  supertype TEXT,
  subtypes TEXT[],
  rarity TEXT,
  artist TEXT,
  types TEXT[],
  language TEXT DEFAULT 'en',
  release_date DATE,
  image_url TEXT,
  set_logo_url TEXT,
  set_symbol_url TEXT,
  is_hot_character BOOLEAN DEFAULT FALSE,
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Recommended indexes:

```sql
CREATE INDEX idx_cards_set_code ON cards(set_code);
CREATE INDEX idx_cards_name_trgm ON cards USING gin (card_name gin_trgm_ops);
```

### `card_search_queries`

A card can have multiple marketplace-specific search queries.

```sql
CREATE TABLE card_search_queries (
  search_query_id BIGSERIAL PRIMARY KEY,
  card_pk BIGINT NOT NULL REFERENCES cards(card_pk),
  marketplace TEXT NOT NULL,                   -- ebay_uk, ebay_us, tcgplayer
  query_text TEXT NOT NULL,
  include_terms TEXT[],
  exclude_terms TEXT[],
  locale TEXT,
  currency TEXT,
  condition_filter TEXT,
  graded_filter BOOLEAN,
  is_primary BOOLEAN DEFAULT TRUE,
  query_version INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  retired_at TIMESTAMPTZ,
  UNIQUE(card_pk, marketplace, query_text, query_version)
);
```

### `card_pricing_profile`

Commercial/tier metadata separated from canonical identity.

```sql
CREATE TABLE card_pricing_profile (
  card_pk BIGINT PRIMARY KEY REFERENCES cards(card_pk),
  clean_value_latest NUMERIC(12,2),
  net_resale_value_latest NUMERIC(12,2),
  price_band TEXT,                              -- low, lower_mid, mid, high
  demand_segment TEXT,                          -- hot, normal, wishlist, inventory
  priority_score INTEGER,
  tier INTEGER,
  scrape_interval_hours INTEGER,
  verified_sales_logged INTEGER,
  price_range_seen_min NUMERIC(12,2),
  price_range_seen_max NUMERIC(12,2),
  review_flag TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### `pricing_jobs`

One scheduler or manual run.

```sql
CREATE TABLE pricing_jobs (
  pricing_job_id UUID PRIMARY KEY,
  job_type TEXT NOT NULL,                       -- scheduled, manual, retry_nulls, backfill
  source TEXT NOT NULL,                         -- scheduler, api, cli
  requested_by TEXT,
  status TEXT NOT NULL,                         -- queued, running, completed, failed, cancelled
  parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
  due_policy_version TEXT,
  scraper_version TEXT,
  cleaner_version TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  error_message TEXT
);
```

### `pricing_job_items`

One card/query/source unit of work.

```sql
CREATE TABLE pricing_job_items (
  pricing_job_item_id UUID PRIMARY KEY,
  pricing_job_id UUID NOT NULL REFERENCES pricing_jobs(pricing_job_id),
  card_pk BIGINT NOT NULL REFERENCES cards(card_pk),
  search_query_id BIGINT REFERENCES card_search_queries(search_query_id),
  source TEXT NOT NULL,                         -- ebay_sold, ebay_active, tcgplayer
  status TEXT NOT NULL,                         -- queued, claimed, running, succeeded, no_results, failed
  priority INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  claimed_by TEXT,
  claimed_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  error_code TEXT,
  error_message TEXT,
  UNIQUE(pricing_job_id, card_pk, source)
);
```

Recommended queue index:

```sql
CREATE INDEX idx_pricing_job_items_queue
  ON pricing_job_items(status, priority DESC, next_retry_at, created_at);
```

### `raw_price_observations`

Immutable source observations. This table replaces scattered raw eBay debug tables and should also hold TCG observations.

```sql
CREATE TABLE raw_price_observations (
  raw_observation_id UUID PRIMARY KEY,
  pricing_job_item_id UUID REFERENCES pricing_job_items(pricing_job_item_id),
  card_pk BIGINT REFERENCES cards(card_pk),
  search_query_id BIGINT REFERENCES card_search_queries(search_query_id),
  source TEXT NOT NULL,                         -- ebay_sold, ebay_active, tcgplayer
  marketplace TEXT,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_date DATE,                              -- sold date or listing date
  title TEXT,
  item_url TEXT,
  source_item_id TEXT,
  price_amount NUMERIC(12,2),
  shipping_amount NUMERIC(12,2),
  total_amount NUMERIC(12,2),
  currency CHAR(3) NOT NULL DEFAULT 'GBP',
  quantity INTEGER,
  condition_text TEXT,
  condition_normalized TEXT,
  language TEXT,
  graded BOOLEAN,
  grade_company TEXT,
  grade_value TEXT,
  finish TEXT,                                  -- normal, holofoil, reverse_holo, unknown
  seller_location TEXT,
  raw_payload JSONB,
  parser_version TEXT,
  included_by_parser BOOLEAN,
  parser_exclusion_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Recommended uniqueness:

```sql
CREATE UNIQUE INDEX uq_raw_observation_source_item
  ON raw_price_observations(source, COALESCE(source_item_id, item_url), event_date, card_pk)
  WHERE source_item_id IS NOT NULL OR item_url IS NOT NULL;
```

### `cleaned_price_snapshots`

Versioned cleaned aggregate. This replaces direct mutable latest-value dependence on `mastercard_v2`.

```sql
CREATE TABLE cleaned_price_snapshots (
  cleaned_snapshot_id UUID PRIMARY KEY,
  card_pk BIGINT NOT NULL REFERENCES cards(card_pk),
  source_group TEXT NOT NULL,                   -- ebay_sold, ebay_active, tcgplayer, blended
  snapshot_date DATE NOT NULL,
  window_start DATE,
  window_end DATE,
  currency CHAR(3) NOT NULL DEFAULT 'GBP',
  median_price NUMERIC(12,2),
  average_price NUMERIC(12,2),
  low_price NUMERIC(12,2),
  high_price NUMERIC(12,2),
  sample_count INTEGER NOT NULL DEFAULT 0,
  included_count INTEGER NOT NULL DEFAULT 0,
  excluded_count INTEGER NOT NULL DEFAULT 0,
  clean_value NUMERIC(12,2),
  confidence_score NUMERIC(5,2),
  trusted BOOLEAN NOT NULL DEFAULT TRUE,
  cleaning_policy_version TEXT NOT NULL,
  source_priority JSONB NOT NULL DEFAULT '[]'::jsonb,
  exclusion_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_from_job_id UUID REFERENCES pricing_jobs(pricing_job_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(card_pk, source_group, snapshot_date, cleaning_policy_version)
);
```

### `cleaned_snapshot_observations`

Bridge table preserving which raw observations contributed to a snapshot.

```sql
CREATE TABLE cleaned_snapshot_observations (
  cleaned_snapshot_id UUID NOT NULL REFERENCES cleaned_price_snapshots(cleaned_snapshot_id),
  raw_observation_id UUID NOT NULL REFERENCES raw_price_observations(raw_observation_id),
  included BOOLEAN NOT NULL,
  exclusion_reason TEXT,
  normalized_price NUMERIC(12,2),
  PRIMARY KEY(cleaned_snapshot_id, raw_observation_id)
);
```

### Supporting tables

```sql
CREATE TABLE pricing_failures (
  pricing_failure_id UUID PRIMARY KEY,
  pricing_job_item_id UUID REFERENCES pricing_job_items(pricing_job_item_id),
  card_pk BIGINT REFERENCES cards(card_pk),
  source TEXT NOT NULL,
  error_code TEXT,
  error_message TEXT,
  url TEXT,
  payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_cards (
  card_pk BIGINT PRIMARY KEY REFERENCES cards(card_pk),
  quantity INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wishlist_cards (
  card_pk BIGINT PRIMARY KEY REFERENCES cards(card_pk),
  priority INTEGER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Mapping current tables/files to V2 tables

| Current asset | V2 destination |
|---|---|
| `mastercard_v2` identity fields | `cards` |
| `mastercard_v2` price/tier columns | `card_pricing_profile` and latest snapshot view |
| `mastercard` legacy rows | `cards.legacy_unique_id` plus staging import table |
| `cards_due.json` | `pricing_jobs` + `pricing_job_items` |
| `dailypricelog` | `cleaned_price_snapshots` with `source_group = 'ebay_sold'`; optionally backfill raw bridge only where URLs exist |
| `activedailypricelog` | `cleaned_price_snapshots` with `source_group = 'ebay_active'` |
| `raw_ebay_sold_debug` | `raw_price_observations` |
| `raw_ebay_active` | `raw_price_observations` |
| `tcg_pricing_log` | `raw_price_observations` and/or `cleaned_price_snapshots` with `source_group = 'tcgplayer'` |
| `ebay_sold_nulls`, `*_retry` | `pricing_job_items.status = 'no_results'` plus `pricing_failures` where applicable |
| `scrape_failures` | `pricing_failures` |
| `trendtracker` | Derived table/materialized view from `cleaned_price_snapshots` |
| `smartsuggestions` | Derived recommendations table/view using snapshots, trends, inventory, wishlist |
| Excel tier tabs/dashboard | BI/report exports from database views |

## Recommended V2 pricing logic

### Clean value precedence

Current logic uses:

1. trusted sold eBay median from last 90 days;
2. active eBay median only if no sold median and at least two active prices;
3. TCG market price, then low price, only if no eBay price exists.

Keep that precedence, but record it explicitly in each snapshot:

```json
{
  "policy": "clean_value_v2",
  "source_priority": ["ebay_sold_90d", "ebay_active", "tcg_market", "tcg_low"],
  "outlier_method": "iqr_1_5",
  "min_active_count": 2
}
```

### Confidence scoring

Add confidence rather than relying on `trusted` alone. Example:

- +40 for sold observations in last 90 days;
- +20 if sample count >= 5;
- +15 if coefficient of variation is low;
- +10 if active and sold medians agree within 20%;
- -20 if only active listings;
- -30 if only TCG fallback;
- -40 if title matching is weak or query is ambiguous.

### Tier/schedule policy

Replace hard-coded intervals with a policy table:

```sql
CREATE TABLE scrape_policies (
  scrape_policy_id BIGSERIAL PRIMARY KEY,
  price_band TEXT,
  demand_segment TEXT,
  min_clean_value NUMERIC(12,2),
  max_clean_value NUMERIC(12,2),
  hot_character BOOLEAN,
  in_inventory BOOLEAN,
  in_wishlist BOOLEAN,
  scrape_interval_hours INTEGER NOT NULL,
  priority_score INTEGER NOT NULL,
  active BOOLEAN DEFAULT TRUE
);
```

Then compute due work from `last_successful_snapshot_at + scrape_interval_hours`.

## Implementation priorities

1. Remove hard-coded credentials and rotate exposed database credentials.
2. Add migrations and define every table used by scripts.
3. Create `pricing_jobs` and `pricing_job_items`; stop using `cards_due.json` as the durable queue.
4. Create `raw_price_observations`; write every eBay and TCG observation there first.
5. Create `cleaned_price_snapshots`; make roll-ups read raw observations and emit versioned snapshots.
6. Add `card_search_queries`; stop deriving query semantics only from `query` string position.
7. Split canonical card data from pricing profile/tier state.
8. Backfill legacy numeric IDs and normalize V2 TCG IDs.
9. Replace duplicated scraper scripts with one library and clear modes: scheduled, manual, retry-null, backfill.
10. Turn workbook outputs into exports generated from database views.

## Final recommendation

This repo should be treated as an early-stage production prototype with valuable domain logic but weak data boundaries. Keep the pricing heuristics, eBay/TCG source knowledge, and workbook-derived operational insights. Move durable state and pipeline lineage into PostgreSQL. JSON should remain a payload/debug/export format only, while cards, jobs, raw observations, and cleaned snapshots become first-class relational tables with explicit keys, constraints, timestamps, and policy versions.
