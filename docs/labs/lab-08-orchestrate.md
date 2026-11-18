# Lab 8 · Orchestrate it

**Time:** 75 minutes · **You will:** wire your steps into one Prefect flow that retries the right errors, catches up after missed runs, backfills any date range, and runs on a schedule.

[All labs](README.md) · Previous: [Lab 7](lab-07-gold-and-load.md) · Next: [Lab 9 · Go to production](lab-09-production.md)

## Why it matters

So far you have run each step by hand. A real pipeline runs unattended, which raises three questions:

1. **Where does the next run start?** Not "the last 30 days" every time, which wastes work and misses older gaps. The pipeline asks the database for the newest date it has, called the **watermark**, and starts from there.
2. **What about data the source revised?** Many sources correct recent values. Each run re-reads a few days before the watermark, called the **overlap**. Upserts (Lab 7) make re-reading safe.
3. **What if a run was missed, or old data was wrong?** A missed run is no problem, because the next run starts from the watermark and **catches up** automatically. To repair an older range, you run a **backfill** with explicit start and end dates.

```text
stored:        ███████████████████|          (watermark = newest stored day)
next run:                     ◄───┼──────►   overlap (re-read 3 days) + new days up to yesterday
backfill:      ◄──────►                      any explicit range, any time
```

## Worked example

### 1. Write the flow

Create `src/my_pipeline/flow.py`:

```python
"""Orchestrate: wire the steps into a Prefect flow with retries, catch-up and backfill."""

import argparse
from datetime import UTC, date, datetime, timedelta

import polars as pl
from prefect import flow, get_run_logger, task

from my_pipeline import config, extract, load
from my_pipeline.validate import validate_weather


def is_temporary(task, task_run, state) -> bool:
    """Tell Prefect to retry only errors that might go away on their own."""
    return isinstance(state.result(raise_on_failure=False), extract.TemporarySourceError)


@task(retries=3, retry_delay_seconds=[5, 30, 60], retry_condition_fn=is_temporary)
def fetch(location: str, start: date, end: date) -> dict:
    latitude, longitude = config.LOCATIONS[location]
    return extract.fetch_weather(latitude, longitude, start, end)


@task
def keep_bronze(payload: dict, location: str) -> None:
    path = extract.save_bronze(payload, location, config.BRONZE_DIR)
    get_run_logger().info("Bronze copy saved to %s", path)


@task
def check(payload: dict, location: str) -> pl.DataFrame:
    return validate_weather(payload, location)


@task
def store(database: str, silver: pl.DataFrame) -> int:
    return load.save(database, silver)


@flow(name="weather-pipeline", log_prints=True)
def weather_pipeline(
    location: str = "accra",
    start: date | None = None,
    end: date | None = None,
    write_bronze: bool = True,
) -> None:
    """Load daily weather for one location.

    With no dates, the flow catches up: it starts a few days before the newest stored
    date (the watermark) and stops at yesterday. Give start and end to backfill a range.
    """
    database = config.DATABASE
    end = end or datetime.now(UTC).date() - timedelta(days=1)  # Today isn't finished yet.
    if start is None:
        latest = load.latest_date(database, location)
        if latest is None:
            start = end - timedelta(days=config.FIRST_LOAD_DAYS)
        else:
            start = latest - timedelta(days=config.OVERLAP_DAYS)
    if start > end:
        raise ValueError(f"start {start} is after end {end}.")
    print(f"Loading {location} from {start} to {end}.")

    payload = fetch(location, start, end)
    if write_bronze:
        keep_bronze(payload, location)
    silver = check(payload, location)
    gold_rows = store(database, silver)
    print(f"Saved {silver.height} Silver rows and refreshed {gold_rows} Gold rows.")


def main() -> None:
    """Command line: run once, backfill a range, or serve on a schedule."""
    parser = argparse.ArgumentParser(description="Load daily weather into the database.")
    parser.add_argument("--location", default="accra", choices=sorted(config.LOCATIONS))
    parser.add_argument("--start", type=date.fromisoformat, help="First day, YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, help="Last day, YYYY-MM-DD")
    parser.add_argument("--serve", action="store_true", help="Run every day at 06:30 UTC")
    args = parser.parse_args()
    if args.serve:
        weather_pipeline.serve(
            name="daily", cron="30 6 * * *", parameters={"location": args.location}
        )
    else:
        weather_pipeline(location=args.location, start=args.start, end=args.end)
    print(load.count_rows(config.DATABASE))
```

What each part does:

- **Thin tasks.** Each task just calls the plain function you already wrote and tested. Prefect adds retries, logs and states. Your logic stays easy to test without Prefect.
- **Retry only temporary errors.** `retry_condition_fn=is_temporary` retries a `TemporarySourceError` and nothing else. The waits grow (5, 30 then 60 seconds), which gives a struggling server time to recover. A `400 Bad Request` or a validation error fails at once, which is what you want.
- **The watermark logic** is the `if start is None` block. The first run (no data yet) loads 30 days. Later runs start `OVERLAP_DAYS` before the newest stored day.
- **`end` defaults to yesterday.** Today isn't finished, so its numbers would change.
- **Explicit dates mean backfill.** Pass `start` and `end`, and the watermark is ignored.

### 2. Point the command at the flow

In `pyproject.toml`, change the script line so `my-pipeline` runs the flow:

```toml
[project.scripts]
my-pipeline = "my_pipeline.flow:main"
```

### 3. Run it: first load, catch-up, backfill

Start the local Prefect server in Terminal 1 (`uv run prefect server start`, with the `local` profile from Lab 3). In Terminal 2:

```sh
uv run my-pipeline
```

```text
Loading accra from 2026-10-18 to 2026-11-17.
Saved 31 Silver rows and refreshed 31 Gold rows.
{'weather_daily': 31, 'weather_features': 31}
```

This is the **first load**: 30 days back from yesterday. Run it again:

```sh
uv run my-pipeline
```

```text
Loading accra from 2026-11-14 to 2026-11-17.
Saved 4 Silver rows and refreshed 31 Gold rows.
{'weather_daily': 31, 'weather_features': 31}
```

This is **catch-up**. The run started 3 days before the watermark (17 November), re-read 4 days, and the counts didn't change. Now **backfill** September:

```sh
uv run my-pipeline --start 2026-09-01 --end 2026-09-30
```

```text
Loading accra from 2026-09-01 to 2026-09-30.
Saved 30 Silver rows and refreshed 61 Gold rows.
{'weather_daily': 61, 'weather_features': 61}
```

Run the backfill a second time and confirm the counts stay at 61. Open each run in the Prefect UI and compare their parameters and logs. Your dates will differ, because they depend on today.

### 4. Simulate a missed run

1. Note the watermark: `uv run python -c "from my_pipeline import config, load; print(load.latest_date(config.DATABASE, 'accra'))"`.
2. Delete the last 5 days, as if the pipeline had been down for a week:

   ```sh
   uv run python -c "import sqlite3; from datetime import date, timedelta; c = sqlite3.connect('data/pipeline.sqlite3'); c.execute('DELETE FROM weather_daily WHERE observed_on > ?', (str(date.today() - timedelta(days=6)),)); c.commit()"
   ```

3. Check the watermark again. It moved back 5 days, for example from `2026-11-17` to `2026-11-12`.
4. Run `uv run my-pipeline`. The log shows it started from the older watermark (minus the overlap) and loaded the missing days, for example `Loading accra from 2026-11-09 to 2026-11-17`. Nobody had to remember what was missed.

### 5. Run it on a schedule

```sh
uv run my-pipeline --serve
```

`serve` registers a deployment called `weather-pipeline/daily` with the local server, then waits and runs it at 06:30 UTC every day. In the UI, open **Deployments → daily** to see upcoming runs, and click **Run → Quick run** to start one now. **The terminal must stay open:** `serve` is the process that does the work. Press `Ctrl + C` to stop it. Lab 9 moves the schedule to the cloud, so nothing has to stay open.

### 6. Test the flow end to end

Create `tests/test_flow.py`. It fakes the source and points the database at a temporary file, so it runs offline in a second:

```python
from datetime import date

from conftest import make_payload

from my_pipeline import config, extract, load
from my_pipeline.flow import weather_pipeline


def test_the_flow_runs_end_to_end_without_the_internet(tmp_path, monkeypatch):
    database = str(tmp_path / "test.sqlite3")
    monkeypatch.setattr(config, "DATABASE", database)
    monkeypatch.setattr(config, "BRONZE_DIR", str(tmp_path / "bronze"))
    monkeypatch.setattr(extract, "fetch_weather", lambda *args: make_payload())

    weather_pipeline(location="accra", start=date(2026, 1, 1), end=date(2026, 1, 3))

    assert load.count_rows(database)["weather_daily"] == 3
    assert len(list((tmp_path / "bronze").rglob("*.json"))) == 1


def test_a_temporary_failure_is_retried(tmp_path, monkeypatch):
    from my_pipeline import flow as flow_module

    calls = []

    def flaky_fetch(*args):
        calls.append(1)
        if len(calls) == 1:
            raise extract.TemporarySourceError("Simulated timeout")
        return make_payload()

    monkeypatch.setattr(config, "DATABASE", str(tmp_path / "test.sqlite3"))
    monkeypatch.setattr(extract, "fetch_weather", flaky_fetch)
    # Don't wait between retries in a test.
    monkeypatch.setattr(flow_module, "fetch", flow_module.fetch.with_options(retry_delay_seconds=0))

    weather_pipeline(start=date(2026, 1, 1), end=date(2026, 1, 3), write_bronze=False)

    assert len(calls) == 2
```

```sh
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Your turn

> [!TIP]
> **Using AI?** Copy-and-fill prompts for this lab are in [AI prompts](ai-prompts.md#lab-8--orchestrate-it). You still need to be able to explain every line you commit.

Build the flow for **your** pipeline:

1. **Tasks** that call your extract, Bronze, validate and load functions.
2. **Retries** only for your `TemporarySourceError`, with growing delays.
3. **A watermark and overlap** that fit your source. A source that revises a week of data needs a week of overlap. A source that never revises can use zero.
4. **Backfill** through `--start` and `--end` (or whatever your source's range looks like: pages, IDs, months).
5. **A schedule** that matches your source's cadence. Daily data needs a daily run. Hourly data might need one every few hours.
6. **Two flow tests**: end to end, and a retry.

Then prove it: do a first load, a catch-up with unchanged counts, a backfill run twice, and a simulated missed run.

Commit:

```sh
git add .
git commit -m "feat: orchestrate the pipeline with catch-up and backfill"
git push
```

## Checkpoint

- [ ] Running the pipeline twice in a row doesn't change the row counts
- [ ] After you delete recent rows, the next run finds and reloads them on its own
- [ ] A backfill of an old range works, and running it twice changes nothing
- [ ] The Prefect UI shows your runs with readable logs, and a scheduled deployment from `serve`
- [ ] `uv run pytest` passes offline

## How the workshop repo does it

- [`historical_ingestion.py`](../../src/market_data/flows/historical_ingestion.py) has the same shape. Its `incremental` mode is catch-up, with a 7-day overlap. `retry_transient` in [`_shared.py`](../../src/market_data/flows/_shared.py) retries only `TransientFetchError`.
- The README exercise [Practice catch-up after missed runs](../../README.md#practice-catch-up-after-missed-runs) walks through an outage, with a table of expected counts.
- [`hourly_ingestion.py`](../../src/market_data/flows/hourly_ingestion.py) does the same for hourly bars, with a 24-hour overlap and an exclusive "completed hour" cutoff.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `Failed to reach API at http://127.0.0.1:4200/api/` | Start the local server (Lab 3), or switch profiles. Tests don't need the server, but the command does |
| Every run reloads 30 days | `latest_date` returns `None`. Check that the flow and `latest_date` use the same database path, and that the location names match exactly |
| A validation error is retried three times | Your retry condition is too broad. Retry only `TemporarySourceError` |
| `start … is after end …` | You passed `--start` without `--end` and the start is after yesterday, or the dates are the wrong way round |
| `serve` runs nothing at the scheduled time | The terminal running `serve` was closed, or the laptop was asleep. That's the limitation Lab 9 fixes |

More fixes: [lab troubleshooting](troubleshooting.md#lab-8--orchestration).

## Go further

- [Prefect: retries and retry conditions](https://www.prefect.io/docs/v3/how-to-guides/workflows/retries)
- Read about **late-arriving data** and **event time vs processing time**: the reason the overlap exists.
