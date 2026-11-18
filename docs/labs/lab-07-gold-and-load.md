# Lab 7 · Gold and an idempotent load

**Time:** 75 minutes · **You will:** compute useful features (Gold) and save Silver and Gold to a database, so that loading the same data twice changes nothing.

[All labs](README.md) · Previous: [Lab 6](lab-06-validate-silver.md) · Next: [Lab 8 · Orchestrate it](lab-08-orchestrate.md)

## Why it matters

Pipelines rerun all the time: after a failure, to repair a range, or because two schedules overlap. If a rerun **duplicates** rows, every count and average downstream is wrong, and nobody notices until much later. A load that gives the same result however many times it runs is called **idempotent**. Three tools make it so:

| Tool | What it does |
| --- | --- |
| **Primary key** | The database refuses two rows with the same key. Use the record key from your plan |
| **Upsert** (`INSERT … ON CONFLICT DO UPDATE`) | Insert new keys and update existing ones, in one statement |
| **Transaction** | Silver and Gold commit together or not at all, so a crash halfway never leaves them out of step |

**Gold** holds values computed from Silver for a specific use: a dashboard, a report or a model. The worked example adds three:

- the daily temperature range
- a 7-day average of the maximum temperature
- a "rainy day" flag

## Worked example

### 1. Compute the features

Create `src/my_pipeline/transform.py`:

```python
"""Transform: compute Gold features from the full Silver history."""

import polars as pl


def build_features(silver: pl.DataFrame) -> pl.DataFrame:
    """Add the values an analyst or a model will use.

    Pass the whole Silver history for a location, not one batch, so that a 7-day
    average at the start of a new batch can still see the days before it.
    """
    return (
        silver.sort("location", "observed_on")
        .with_columns(
            (pl.col("temp_max_c") - pl.col("temp_min_c")).round(1).alias("temp_range_c"),
            pl.col("temp_max_c")
            .rolling_mean(7)
            .over("location")
            .round(2)
            .alias("temp_max_7d_avg_c"),
            (pl.col("precipitation_mm") >= 1.0).alias("is_rainy_day"),
        )
        .select("location", "observed_on", "temp_range_c", "temp_max_7d_avg_c", "is_rainy_day")
    )
```

- `.over("location")` computes the rolling average separately for each location, so Accra's week never mixes with Kumasi's.
- The first six days have no 7-day average, so they are `null`. That's honest. Don't fill gaps with invented numbers.
- **Why the full history?** Imagine tomorrow's run loads one new day. Its 7-day average needs the six days *before* it, and those are already in the database, not in tomorrow's batch. So the load step rebuilds Gold from all stored Silver.

### 2. Save it idempotently

Create `src/my_pipeline/load.py`:

```python
"""Load: save Silver and Gold so that loading the same batch twice changes nothing."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date
from pathlib import Path

import polars as pl

from my_pipeline.transform import build_features

sqlite3.register_adapter(date, date.isoformat)  # Store dates as YYYY-MM-DD text in SQLite.

TABLES = """
CREATE TABLE IF NOT EXISTS weather_daily (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_max_c DOUBLE PRECISION NOT NULL,
    temp_min_c DOUBLE PRECISION NOT NULL,
    precipitation_mm DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (location, observed_on)
);
CREATE TABLE IF NOT EXISTS weather_features (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_range_c DOUBLE PRECISION NOT NULL,
    temp_max_7d_avg_c DOUBLE PRECISION,
    is_rainy_day BOOLEAN NOT NULL,
    PRIMARY KEY (location, observed_on)
);
"""
# {p} becomes the driver's placeholder: ? for SQLite (and %s for PostgreSQL in Lab 9).
UPSERT_SILVER = """
INSERT INTO weather_daily (location, observed_on, temp_max_c, temp_min_c, precipitation_mm)
VALUES ({p}, {p}, {p}, {p}, {p})
ON CONFLICT (location, observed_on) DO UPDATE SET
    temp_max_c = excluded.temp_max_c,
    temp_min_c = excluded.temp_min_c,
    precipitation_mm = excluded.precipitation_mm,
    loaded_at = CURRENT_TIMESTAMP
"""
UPSERT_GOLD = """
INSERT INTO weather_features
    (location, observed_on, temp_range_c, temp_max_7d_avg_c, is_rainy_day)
VALUES ({p}, {p}, {p}, {p}, {p})
ON CONFLICT (location, observed_on) DO UPDATE SET
    temp_range_c = excluded.temp_range_c,
    temp_max_7d_avg_c = excluded.temp_max_7d_avg_c,
    is_rainy_day = excluded.is_rainy_day
"""
SILVER_COLUMNS = ["location", "observed_on", "temp_max_c", "temp_min_c", "precipitation_mm"]


@contextmanager
def open_database(database: str) -> Iterator[tuple[sqlite3.Cursor, str]]:
    """Open the database, create the tables, and commit only if everything succeeds."""
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(TABLES)
        with connection:  # Commits on success, rolls back on any error.
            yield connection.cursor(), "?"


def save(database: str, silver: pl.DataFrame) -> int:
    """Upsert Silver, rebuild Gold from all stored Silver, and commit both together.

    Returns:
        The number of Gold rows written.
    """
    locations = silver["location"].unique().to_list()
    with open_database(database) as (cursor, p):
        cursor.executemany(UPSERT_SILVER.format(p=p), silver.select(SILVER_COLUMNS).rows())
        query = f"SELECT {', '.join(SILVER_COLUMNS)} FROM weather_daily WHERE location = {p}"
        history = []
        for location in locations:
            cursor.execute(query, (location,))
            history += cursor.fetchall()
        stored = pl.DataFrame(history, schema=SILVER_COLUMNS, orient="row").with_columns(
            pl.col("observed_on").cast(pl.String).str.to_date()
        )
        gold = build_features(stored)
        cursor.executemany(UPSERT_GOLD.format(p=p), gold.rows())
    return gold.height


def latest_date(database: str, location: str) -> date | None:
    """Return the newest stored date for a location: the pipeline's watermark."""
    with open_database(database) as (cursor, p):
        cursor.execute(
            f"SELECT MAX(observed_on) FROM weather_daily WHERE location = {p}", (location,)
        )
        (latest,) = cursor.fetchone()
    return date.fromisoformat(str(latest)) if latest else None


def count_rows(database: str) -> dict[str, int]:
    """Count the rows in each table: a quick check that reruns add nothing."""
    with open_database(database) as (cursor, _):
        counts = {}
        for table in ("weather_daily", "weather_features"):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts
```

The important parts:

- **`PRIMARY KEY (location, observed_on)`** is your record key from Lab 1, enforced by the database.
- **`ON CONFLICT … DO UPDATE`** turns "insert" into "insert or update". `excluded` means "the row we tried to insert". This one statement is what makes reruns safe.
- **`with connection:`** wraps Silver *and* Gold in one transaction. If anything fails, both roll back.
- **Placeholders, never f-strings, for values.** `?` lets the database driver insert values safely. Building SQL with f-strings from data invites *SQL injection*. The f-strings above only fill in fixed table names and the placeholder symbol, never data.
- **`latest_date`** returns the newest stored date. Lab 8 uses it as a *watermark* to decide where the next run starts.
- The SQL uses plain types (`TEXT`, `DATE`, `DOUBLE PRECISION`, `BOOLEAN`) that both SQLite and PostgreSQL accept, so Lab 9 needs only a few changes.

### 3. Run Extract → Validate → Load by hand

```text
uv run python
>>> from datetime import date
>>> from my_pipeline import config, extract, load
>>> from my_pipeline.validate import validate_weather
>>> payload = extract.fetch_weather(*config.LOCATIONS["accra"], date(2026, 10, 1), date(2026, 10, 31))
>>> silver = validate_weather(payload, "accra")
>>> load.save(config.DATABASE, silver)
31
>>> load.count_rows(config.DATABASE)
{'weather_daily': 31, 'weather_features': 31}
>>> load.save(config.DATABASE, silver)
31
>>> load.count_rows(config.DATABASE)
{'weather_daily': 31, 'weather_features': 31}
```

You loaded the same 31 days twice and still have 31 rows. **That's idempotency.** Now look at the Gold table:

```text
>>> import sqlite3
>>> connection = sqlite3.connect(config.DATABASE)
>>> for row in connection.execute("SELECT * FROM weather_features ORDER BY observed_on DESC LIMIT 3"):
...     print(row)
...
('accra', '2026-10-31', 8.1, 32.46, 0)
('accra', '2026-10-30', 5.5, 32.59, 1)
('accra', '2026-10-29', 6.8, 32.7, 0)
```

SQLite stores booleans as `0` and `1`. To browse the tables visually, install the **SQLite Viewer** extension in VS Code and open `data/pipeline.sqlite3`.

### 4. Test it

Create `tests/test_transform.py`:

```python
import polars as pl
from conftest import make_payload

from my_pipeline.transform import build_features
from my_pipeline.validate import validate_weather


def test_features_are_computed_per_row():
    gold = build_features(validate_weather(make_payload(), "accra"))
    assert gold["temp_range_c"].to_list() == [7.0, 7.0, 7.0]
    assert gold["is_rainy_day"].to_list() == [False, True, False]


def test_the_7_day_average_waits_for_7_days():
    gold = build_features(validate_weather(make_payload(days=8), "accra"))
    averages = gold["temp_max_7d_avg_c"].to_list()
    assert averages[:6] == [None] * 6
    assert averages[6] == 34.0  # mean of 31, 32, ... 37
    assert gold.filter(pl.col("temp_max_7d_avg_c").is_not_null()).height == 2
```

Create `tests/test_load.py`. It is the most important test in your project:

```python
from conftest import make_payload

from my_pipeline import load
from my_pipeline.validate import validate_weather


def test_loading_the_same_batch_twice_changes_nothing(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    silver = validate_weather(make_payload(), "accra")

    load.save(database, silver)
    load.save(database, silver)

    assert load.count_rows(database) == {"weather_daily": 3, "weather_features": 3}
    assert str(load.latest_date(database, "accra")) == "2026-01-03"
```

```sh
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Your turn

For **your** source:

1. **Design Gold.** Pick two or three values someone would actually use, such as a change from the previous record, a rolling average, a flag or a category. Write each one in `transform.py`, and test at least one by hand-computing the expected answer.
2. **Create both tables** with your record key as the `PRIMARY KEY`.
3. **Write the two upserts.** Every column except the key goes in `DO UPDATE SET`.
4. **Prove idempotency.** Load the same batch twice, by hand and in a test, and check that the counts don't change.
5. **Break the transaction on purpose.** Temporarily add `raise RuntimeError("boom")` inside `save`, just before the Gold upsert. Run a load and check that Silver didn't change either. Then remove the line.

Commit:

```sh
git add .
git commit -m "feat: load Silver and Gold idempotently"
git push
```

## Checkpoint

- [ ] Loading the same batch twice leaves the row counts unchanged
- [ ] A failure halfway through `save` leaves both tables as they were
- [ ] Your Gold table has at least two features, and at least one has a test with a hand-checked answer

## How the workshop repo does it

- [`storage.py`](../../src/market_data/ingestion/storage.py) has the same upserts, for SQLite and PostgreSQL, and commits Silver and Gold in one transaction.
- [`features.py`](../../src/market_data/ingestion/features.py) computes returns, moving averages and volatility with `.over("symbol")`. Like this lab, it rebuilds Gold from the stored Silver history.
- README: [Inspect SQLite and prove repeatability](../../README.md#inspect-sqlite-and-prove-repeatability) runs the same check, at a larger size.
- [Architecture: storage and repeatability](../ARCHITECTURE.md#storage-and-repeatability) explains the design.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `UNIQUE constraint failed` | You used plain `INSERT`. Add the `ON CONFLICT (…) DO UPDATE SET …` part |
| `ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint` | The columns in `ON CONFLICT (…)` must be exactly the primary key columns. If you changed the key, delete the old database file so the table is recreated |
| `sqlite3.ProgrammingError: Incorrect number of bindings` | The number of `{p}` placeholders must equal the number of columns, in the same order as `.rows()` returns them |
| `database is locked` | Another program has the file open, such as a viewer or a second terminal. Close it |
| Gold has fewer rows than Silver, or averages look wrong | Make sure you rebuild Gold from *stored* Silver, sorted by date |

More fixes: [lab troubleshooting](troubleshooting.md#labs-57--extract-validate-load).

## Go further

- [SQLite: UPSERT](https://sqlite.org/lang_upsert.html) and [PostgreSQL: INSERT … ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- What happens to a 7-day average when a day is missing? The rolling window counts *rows*, not calendar days. Write down the limitation in your README, or look up `rolling_mean_by` in Polars.
