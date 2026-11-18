# Lab 5 · Extract into Bronze

**Time:** 60 minutes · **You will:** read your real source and save an untouched copy of every response, with notes on where and when it came from.

[All labs](README.md) · Previous: [Lab 4](lab-04-deploy-hello.md) · Next: [Lab 6 · Validate into Silver](lab-06-validate-silver.md)

## Why it matters

**Bronze** is the raw layer: what the source sent, exactly as it sent it. It looks wasteful until the day you need it.

- **Replay.** If you find a bug in your cleaning code next month, fix it and rebuild from Bronze. You don't need to ask the source again, and the source may not even have the old data any more.
- **Evidence.** When a number looks wrong, Bronze shows whether the source sent it or your code made it.
- **Schema drift.** When the source renames a field, the old and new shapes are both in Bronze, side by side.

So Bronze **never changes or filters anything**. It only adds metadata: where the data came from (`_source`) and when you got it (`_ingested_at`).

The second idea in this lab is that **errors come in two kinds**:

| Kind | Examples | What to do |
| --- | --- | --- |
| **Temporary** | Timeout, connection reset, `429 Too Many Requests`, `5xx` server error | Retry later. It will probably work |
| **Permanent** | `400 Bad Request`, `401 Unauthorized`, `404 Not Found`, a bug in your code | Stop and tell a human. Retrying gives the same answer |

Your extract code should raise *different exceptions* for the two kinds, so that Lab 8 can retry only the temporary ones.

## Worked example

### 1. Put settings in one place

Create `src/my_pipeline/config.py`:

```python
"""Settings in one place. Values come from the environment or .env, with safe defaults."""

import os

from dotenv import load_dotenv

load_dotenv()  # Reads .env from the folder you run the command in, if it exists.

DATABASE = os.getenv("DATABASE_URL") or "data/pipeline.sqlite3"
BRONZE_DIR = os.getenv("BRONZE_DIR") or "data/bronze"

# Latitude and longitude for each place we collect. Add your own.
LOCATIONS = {
    "accra": (5.56, -0.20),
    "kumasi": (6.69, -1.62),
}
FIRST_LOAD_DAYS = 30  # How far back the very first run reaches.
OVERLAP_DAYS = 3  # Later runs re-read this many recent days, because the source revises them.
```

Settings that change between your laptop and production (the database, folders, keys) come from the environment. Settings that describe the pipeline (which places, how far back) are plain constants you review in Git.

### 2. Write the extract step

Create `src/my_pipeline/extract.py`:

```python
"""Extract: ask the source for data and keep an untouched Bronze copy."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import httpx

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_FIELDS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]


class TemporarySourceError(RuntimeError):
    """The source failed in a way that might work if we try again."""


def fetch_weather(latitude: float, longitude: float, start: date, end: date) -> dict:
    """Download daily weather for one place, from start to end (both included).

    Raises:
        TemporarySourceError: Network trouble, rate limiting or a server error.
        httpx.HTTPStatusError: Any other bad answer, such as a wrong parameter.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_FIELDS),
        "timezone": "UTC",
    }
    try:
        response = httpx.get(ARCHIVE_URL, params=params, timeout=30)
    except httpx.TransportError as error:
        raise TemporarySourceError(f"Could not reach Open-Meteo: {error}") from error
    if response.status_code == 429 or response.status_code >= 500:
        raise TemporarySourceError(f"Open-Meteo answered {response.status_code}. Try later.")
    response.raise_for_status()
    return response.json()


def save_bronze(payload: dict, location: str, bronze_dir: str) -> Path:
    """Save the response exactly as received, plus metadata about where it came from."""
    now = datetime.now(UTC)
    folder = Path(bronze_dir) / f"ingest_date={now.date()}" / f"location={location}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{uuid4().hex}.json"
    record = {
        "_source": ARCHIVE_URL,
        "_location": location,
        "_ingested_at": now.isoformat(),
        "payload": payload,
    }
    path.write_text(json.dumps(record, indent=2))
    return path
```

Notice:

- **`timeout=30`.** Without a timeout, a stuck server can hang your pipeline forever.
- **Two kinds of errors.** Network problems, 429 and 5xx become `TemporarySourceError`. `raise_for_status()` turns any other 4xx into `httpx.HTTPStatusError`, which is permanent.
- **Bronze is append-only.** Every save gets a new random file name (`uuid4`), so nothing is ever overwritten. Folders named `ingest_date=…/location=…` are a common convention called *partitioning*. Tools like Polars and Spark can read just the folders they need.
- **Times are in UTC**, so runs in different time zones never disagree about "today".

### 3. Try it by hand

Start Python inside the project environment and type the lines after `>>>`:

```text
uv run python
>>> from datetime import date
>>> from my_pipeline import config, extract
>>> payload = extract.fetch_weather(*config.LOCATIONS["accra"], date(2026, 11, 1), date(2026, 11, 7))
>>> payload["daily"]["time"]
['2026-11-01', '2026-11-02', '2026-11-03', '2026-11-04', '2026-11-05', '2026-11-06', '2026-11-07']
>>> extract.save_bronze(payload, "accra", config.BRONZE_DIR)
PosixPath('data/bronze/ingest_date=2026-11-18/location=accra/a5b1dfaa….json')
>>> exit()
```

Open the JSON file in VS Code. The original response sits under `payload`, wrapped in the metadata.

Now trigger a **permanent** error by putting the end date before the start date:

```text
>>> extract.fetch_weather(5.56, -0.20, date(2026, 11, 7), date(2026, 11, 1))
httpx.HTTPStatusError: Client error '400 Bad Request' for url ...
```

Retrying this would never help, and the error says so.

### 4. Test it without the internet

Tests that call a real API are slow and fail whenever the API is down. Replace the network call with a fake one instead. Create `tests/test_extract.py`:

```python
import json
from datetime import date

import httpx
import pytest

from my_pipeline import extract


def test_bronze_keeps_the_payload_and_where_it_came_from(tmp_path):
    payload = {"daily": {"time": ["2026-01-01"], "temperature_2m_max": [31.0]}}

    path = extract.save_bronze(payload, "accra", str(tmp_path))

    record = json.loads(path.read_text())
    assert record["payload"] == payload
    assert record["_location"] == "accra"
    assert "_ingested_at" in record
    assert "location=accra" in str(path)


def fake_answer(status_code):
    """Replace httpx.get with a function that returns a fixed status, without the internet."""

    def fake_get(url, params, timeout):
        return httpx.Response(status_code, json={}, request=httpx.Request("GET", url))

    return fake_get


def test_a_server_error_is_temporary(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(503))
    with pytest.raises(extract.TemporarySourceError):
        extract.fetch_weather(5.56, -0.20, date(2026, 1, 1), date(2026, 1, 3))


def test_a_bad_request_is_permanent(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(400))
    with pytest.raises(httpx.HTTPStatusError):
        extract.fetch_weather(5.56, -0.20, date(2026, 1, 1), date(2026, 1, 3))
```

- `tmp_path` is a pytest fixture that gives each test its own empty folder, deleted afterwards.
- `monkeypatch.setattr` swaps a function for the length of one test. Here the fake `httpx.get` returns whatever status you want.

```sh
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Your turn

> [!TIP]
> **Using AI?** Copy-and-fill prompts for this lab are in [AI prompts](ai-prompts.md#lab-5--extract-into-bronze). You still need to be able to explain every line you commit.

Write the extract step for **your** source, in your own `extract.py` (and your own `config.py`).

1. **A fetch function** that takes the parameters your plan needs (dates, a city, a currency…) and returns what the source sent. Don't clean anything yet.
2. **Two kinds of errors.** Raise your own `TemporarySourceError` for timeouts, 429 and 5xx. Let everything else fail normally.
3. **A `save_bronze` function** that stores the raw response with at least `_source` and `_ingested_at`.
4. **Two tests**: Bronze keeps the payload, and a 503 becomes `TemporarySourceError`.

Hints for different sources:

| Your source | Tips |
| --- | --- |
| API that needs a key | Put `MY_API_KEY=` in `.env.example`, the real key in `.env`, and read it in `config.py` with `os.getenv("MY_API_KEY")`. **Never write the key in code** |
| API with pages | Loop until the response has no "next page" link, collect every page, and save them together (or one Bronze file per page) |
| A CSV or Excel file | Your "fetch" reads the file. Bronze is a copy of the file, with the metadata in the file name or a small JSON file beside it |
| Data as rows, not columns | Fine. Bronze doesn't care about shape. Lab 6 handles it |

Commit:

```sh
git add .
git commit -m "feat: extract the source into Bronze"
git push
```

## Checkpoint

- [ ] Running your fetch by hand returns real data, and a Bronze file appears under `data/bronze/`
- [ ] Opening the Bronze file shows the source's response unchanged, plus your metadata
- [ ] `uv run pytest` passes with your Wi-Fi switched off
- [ ] `git status` doesn't show anything under `data/` (it's ignored)

## How the workshop repo does it

- [`sources.py`](../../src/market_data/ingestion/sources.py) fetches from Yahoo Finance. It wraps timeouts and rate limits in a `TransientFetchError`, and it can read a packaged fixture file instead of the API.
- Its Bronze is Parquet, not JSON, with `_symbol`, `_source` and `_ingested_at` columns. See `save_snapshot` in the same file.
- The README section [Five-minute fixture quickstart](../../README.md#five-minute-fixture-quickstart) shows how to read a Bronze file with Polars.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `httpx.ConnectError` or `ConnectTimeout` | You're offline, or a proxy blocks the API. Try the URL in a browser. If the browser works but Python doesn't, you're probably behind a proxy |
| `400 Bad Request` | A parameter is wrong. Print the URL with `print(response.url)` and compare it with the API docs |
| `401` or `403` | The API key is missing or wrong. Check that `.env` has it and that `config.py` reads the right name |
| `429 Too Many Requests` | You're calling too often. Fetch bigger date ranges less often, and don't loop over hundreds of requests during development |
| `KeyError` when reading the response | Print `payload.keys()` and look at the real shape. APIs often wrap data in `data`, `results` or `items` |

More fixes: [lab troubleshooting](troubleshooting.md#labs-57--extract-validate-load).

## Go further

- [HTTPX quickstart](https://www.python-httpx.org/quickstart/), especially timeouts and exceptions
- [HTTP status codes on MDN](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status)
- [Bronze, Silver and Gold layers](https://www.databricks.com/glossary/medallion-architecture), from Databricks
