# Lab 6 · Validate into Silver

**Time:** 60 minutes · **You will:** turn raw responses into a typed table you can trust, and reject any batch that breaks your rules.

[All labs](README.md) · Previous: [Lab 5](lab-05-extract-bronze.md) · Next: [Lab 7 · Gold and an idempotent load](lab-07-gold-and-load.md)

## Why it matters

**Silver** is the layer people trust. Everything downstream (dashboards, reports, models) assumes that a Silver column called `temp_max_c` holds a number, in degrees Celsius, for every row, with no duplicates. Those promises are your **data contract**. The validation step is where you enforce it.

A contract has three parts:

1. **Fields.** Which columns must exist.
2. **Types.** Dates are dates, numbers are numbers.
3. **Rules.** Facts about the world: the minimum temperature can't be above the maximum, rainfall can't be negative, and one location has one row per day.

When a batch breaks the contract, this lab **fails fast**: it rejects the whole batch with a clear message, and Silver stays unchanged. The Bronze copy is still there, so nothing is lost. Once the cause is fixed, you replay it. ([Schema drift](../SCHEMA_DRIFT.md#2-how-a-batch-is-handled) covers the alternatives, quarantine and rescue.)

## Worked example

### 1. Write the contract as code

Create `src/my_pipeline/validate.py`:

```python
"""Validate: turn the raw response into a typed Silver table, or reject the whole batch."""

import polars as pl

FIELDS = {
    "time": "observed_on",
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "precipitation_sum": "precipitation_mm",
}
KEY = ["location", "observed_on"]


def validate_weather(payload: dict, location: str) -> pl.DataFrame:
    """Check the data contract and return clean Silver rows.

    Raises:
        ValueError: A field is missing or any row breaks a rule. One bad row rejects the batch.
    """
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        daily = {}  # Treat a missing or malformed section as "every field is missing".
    missing = sorted(FIELDS.keys() - daily.keys())
    if missing:
        raise ValueError(f"{location}: missing fields: {', '.join(missing)}.")
    if len({len(daily[name]) for name in FIELDS}) != 1:
        raise ValueError(f"{location}: the fields have different numbers of values.")

    raw = pl.DataFrame({name: daily[name] for name in FIELDS}, strict=False)
    silver = raw.select(
        pl.lit(location).alias("location"),
        pl.col("time").cast(pl.String).str.to_date("%Y-%m-%d", strict=False).alias("observed_on"),
        *[
            pl.col(source).cast(pl.Float64, strict=False).alias(target)
            for source, target in FIELDS.items()
            if source != "time"
        ],
    )

    temperatures = pl.col("temp_max_c", "temp_min_c")
    rules = {
        "a value is missing or has the wrong type": pl.any_horizontal(pl.all().is_null()),
        "temperature is outside -90 to 60 °C": pl.any_horizontal(~temperatures.is_between(-90, 60)),
        "the minimum temperature is above the maximum": pl.col("temp_min_c") > pl.col("temp_max_c"),
        "precipitation is negative": pl.col("precipitation_mm") < 0,
        "the same location and date appear twice": pl.struct(KEY).is_duplicated(),
    }
    for problem, broken in rules.items():
        bad_rows = silver.filter(broken)
        if bad_rows.height:
            examples = bad_rows["observed_on"].head(3).to_list()
            raise ValueError(f"{location}: {problem} in {bad_rows.height} row(s), e.g. {examples}.")
    return silver.sort(KEY)
```

How it works:

- **Fields first.** Missing fields are reported by name before anything else runs.
- **Rename to your own names.** `FIELDS` maps the source's names to Silver's names. The rest of your pipeline uses only Silver's names, so if the source renames a field, you change one line.
- **Types with `strict=False`.** A value that can't be converted becomes `null` instead of crashing. The first rule then catches every `null`, so a bad value fails *with a useful message*.
- **Rules as a dictionary.** Each entry is "what's wrong" → "a Polars condition that is true for bad rows". Adding a rule means adding one line, and the message is written for you.
- **Examples in the error.** The message names up to three bad dates, so you can find them in Bronze straight away.

### 2. Replay Bronze through it

Bronze from Lab 5 is real input. Validate it:

```text
uv run python
>>> import json
>>> from pathlib import Path
>>> from my_pipeline.validate import validate_weather
>>> path = next(Path("data/bronze").rglob("*.json"))
>>> record = json.loads(path.read_text())
>>> validate_weather(record["payload"], record["_location"]).head(3)
shape: (3, 5)
┌──────────┬─────────────┬────────────┬────────────┬──────────────────┐
│ location ┆ observed_on ┆ temp_max_c ┆ temp_min_c ┆ precipitation_mm │
│ str      ┆ date        ┆ f64        ┆ f64        ┆ f64              │
╞══════════╪═════════════╪════════════╪════════════╪══════════════════╡
│ accra    ┆ 2026-11-01  ┆ 31.8       ┆ 24.9       ┆ 0.0              │
…
```

Now break one value and watch the batch get rejected:

```text
>>> record["payload"]["daily"]["temperature_2m_min"][2] = 99.0
>>> validate_weather(record["payload"], record["_location"])
ValueError: accra: temperature is outside -90 to 60 °C in 1 row(s), e.g. [datetime.date(2026, 11, 3)].
```

This is the **replay** idea from Lab 5 in action. Validation reads Bronze, so you can rerun it on old data at any time.

### 3. Test every rule

Good tests show that a good batch passes **and** that each rule catches what it should. First add a helper that builds a small fake response. Open `tests/conftest.py` from Lab 3 and add this at the end:

```python
def make_payload(days: int = 3, **changes) -> dict:
    """Build a response shaped like Open-Meteo's, with optional changed fields."""
    daily = {
        "time": [f"2026-01-{day:02d}" for day in range(1, days + 1)],
        "temperature_2m_max": [31.0 + day for day in range(days)],
        "temperature_2m_min": [24.0 + day for day in range(days)],
        "precipitation_sum": [0.0, 2.5, 0.4][:days] + [0.0] * max(0, days - 3),
    }
    daily.update(changes)
    return {"daily": daily}
```

Create `tests/test_validate.py`:

```python
import pytest
from conftest import make_payload

from my_pipeline.validate import validate_weather


def test_a_good_batch_becomes_typed_silver_rows():
    silver = validate_weather(make_payload(), "accra")
    assert silver.height == 3
    assert silver.columns == [
        "location",
        "observed_on",
        "temp_max_c",
        "temp_min_c",
        "precipitation_mm",
    ]
    assert str(silver["observed_on"].dtype) == "Date"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"temperature_2m_min": [40.0, 24.0, 25.0]}, "minimum temperature is above"),
        ({"precipitation_sum": [0.0, -1.0, 0.0]}, "precipitation is negative"),
        ({"temperature_2m_max": [31.0, None, 33.0]}, "missing or has the wrong type"),
        ({"time": ["2026-01-01", "2026-01-01", "2026-01-03"]}, "appear twice"),
        ({"time": ["2026-01-01", "2026-01-02"]}, "different numbers of values"),
    ],
)
def test_one_bad_row_rejects_the_whole_batch(changes, message):
    with pytest.raises(ValueError, match=message):
        validate_weather(make_payload(**changes), "accra")


def test_a_missing_field_is_named():
    payload = make_payload()
    del payload["daily"]["precipitation_sum"]
    with pytest.raises(ValueError, match="missing fields: precipitation_sum"):
        validate_weather(payload, "accra")
```

`@pytest.mark.parametrize` runs one test function once per row of the table, so five broken batches need only one test.

```sh
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Your turn

Write `validate.py` for **your** source, using the contract from your Lab 1 plan.

1. **Name the fields** your Silver table promises, and map the source's names to yours.
2. **Convert types**: dates, numbers, text. Use `strict=False`, then reject `null`s with a clear message.
3. **Write at least three rules**, one of them on your record key (no duplicates).
4. **Test** a good batch, each rule, and a missing field.
5. **Replay** a real Bronze file through it.

Hints for different shapes:

| Your Bronze looks like | Build the frame with |
| --- | --- |
| A dict of lists (like Open-Meteo) | `pl.DataFrame(payload["daily"])` |
| A list of records: `[{"date": …, "rate": …}, …]` | `pl.DataFrame(payload["results"])` |
| Nested dicts, e.g. `{"rates": {"2026-11-01": {"EUR": 0.89}}}` | Flatten with a loop first: `rows = [{"date": d, **values} for d, values in payload["rates"].items()]` |
| A CSV file | `pl.read_csv(path)`, then cast each column |

Commit:

```sh
git add .
git commit -m "feat: validate Bronze into Silver"
git push
```

## Checkpoint

- [ ] A real Bronze file validates into a Silver table with the column names and types you planned
- [ ] Changing one value in a copy of the payload produces an error that names the problem and the row
- [ ] Each rule has a test, and `uv run pytest` passes

## How the workshop repo does it

- [`validation.py`](../../src/market_data/ingestion/validation.py) uses the same pattern for price bars. It has a `SOURCE_COLUMNS` mapping, a dictionary of rules, and errors with example dates.
- Its rules are domain-specific: prices must be positive and finite, `low` can't be above `open` or `close`, and volume must be a whole number.
- For hourly data it also keeps fields it doesn't recognize, instead of dropping them. That's **rescue**, covered in Lab 10.
- [Schema drift §2](../SCHEMA_DRIFT.md#two-ways-to-write-a-contract-polars-and-pydantic) compares this column-level Polars style with row-level Pydantic models.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `ShapeError` when building the DataFrame | The lists have different lengths. Check that first, as the worked example does |
| Every row fails "missing or has the wrong type" | A type conversion failed for the whole column. Print `raw.head()` and check the date format (`%Y-%m-%d`, `%d/%m/%Y`, …) or a number stored as text with commas |
| `ModuleNotFoundError: No module named 'conftest'` | Run `uv run pytest` from the project root, and keep `conftest.py` inside `tests/` |
| A rule seems to catch nothing | Test it: build a payload that breaks *only* that rule and check that `pytest.raises` matches its message |

More fixes: [lab troubleshooting](troubleshooting.md#labs-57--extract-validate-load).

## Go further

- [Polars user guide: expressions](https://docs.pola.rs/user-guide/concepts/expressions-and-contexts/)
- Try the Pydantic exercise in [Schema drift §6](../SCHEMA_DRIFT.md#6-practice).
- Read about **Great Expectations** or **Pandera**, libraries that turn rules like these into reusable checks.
