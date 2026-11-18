# Lab 1 · Choose your data and plan the pipeline

**Time:** 45 minutes · **You will:** pick a data source and write a one-page plan. No code yet.

[All labs](README.md) · Previous: [Lab 0](lab-00-setup.md) · Next: [Lab 2 · Create the project](lab-02-project.md)

## Why it matters

Most pipeline bugs are planning bugs. Common ones are not knowing what makes a record unique, not knowing when the source updates, and never deciding what "valid" means. Ten minutes of planning saves hours of debugging. Every later lab refers back to the plan you write here.

## Steps

### 1. Pick a source

A good first source:

- **is free and needs no paid key**
- **has a clear record key**, meaning the columns that identify one record, such as `city + date` or `currency + date`
- **has numbers you can check**, such as a temperature, a price or a count
- **keeps changing**, so a schedule makes sense
- **contains no personal data**

Some sources that fit:

| Source | What you get | Record key | Notes |
| --- | --- | --- | --- |
| [Open-Meteo historical weather](https://open-meteo.com/en/docs/historical-weather-api) | Daily or hourly weather for any coordinates | `location + date` | **The worked example in these labs.** No key |
| [Open-Meteo air quality](https://open-meteo.com/en/docs/air-quality-api) | Hourly PM2.5, PM10 and ozone | `location + hour` | No key |
| [Frankfurter](https://frankfurter.dev/) | Daily European Central Bank exchange rates | `base + currency + date` | No key. Covers about 30 currencies, so check that yours is listed |
| [USGS earthquakes](https://earthquake.usgs.gov/fdsnws/event/1/) | Every earthquake, with magnitude and location | `event id` | No key. Records can be revised after they first appear |
| A CSV file you download regularly | Anything: sales, transport, sports, school data | You decide | Works offline. Your "extract" step reads a file instead of an API |

You can use something else. Check that you're allowed to use the data (read its terms) and that you can explain it to a classmate.

### 2. Look at the raw data

Before you write any code, look at what the source actually sends. For an API, open the URL in your browser or use `curl`. The worked example:

```sh
curl "https://archive-api.open-meteo.com/v1/archive?latitude=5.56&longitude=-0.20&start_date=2026-11-01&end_date=2026-11-03&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=UTC"
```

The response looks like this (shortened):

```json
{
  "daily_units": { "time": "iso8601", "temperature_2m_max": "°C", "precipitation_sum": "mm" },
  "daily": {
    "time": ["2026-11-01", "2026-11-02", "2026-11-03"],
    "temperature_2m_max": [31.8, 32.4, 32.0],
    "temperature_2m_min": [24.9, 25.3, 25.1],
    "precipitation_sum": [0.0, 3.1, 0.4]
  }
}
```

Write down what you notice. For the example: the data comes as **columns** (one list per field), not as rows. Dates are text. Units sit in a separate section. The most recent days can still change for a while, because the provider revises them.

### 3. Write your plan

Create a text file somewhere handy (you'll move it into your project in Lab 2) and fill in this template. The weather answers are shown as an example.

```markdown
# Pipeline plan

## Source
- What: daily weather (max/min temperature, rainfall) for Accra and Kumasi
- Where: Open-Meteo historical API, no key needed
- Format: JSON, one list per field
- Terms: free for non-commercial use

## Record key
- One record = one location on one day
- Key: location + observed_on

## Data contract (what "valid" means)
- Required fields: time, temperature_2m_max, temperature_2m_min, precipitation_sum
- Types: date, number, number, number
- Rules:
  1. temperatures between -90 and 60 °C
  2. min temperature <= max temperature
  3. rainfall >= 0
  4. no duplicate location + date

## Layers
- Bronze: the JSON response exactly as received, plus where and when we got it
- Silver: one typed row per location and day
- Gold: daily temperature range, 7-day average max temperature, rainy-day flag

## Cadence
- The source adds one day per day and revises recent days for a while
- Run once a day, re-reading the last 3 days each time

## What could go wrong
- API down or slow -> retry
- A field renamed or removed -> reject the batch, keep the Bronze copy
- A missed day -> the next run catches up
```

### Your turn

Fill in the template for **your** source. Be specific about the record key and the rules. If you can't name a record key, pick a different source, because Lab 7 depends on it.

## Checkpoint

- [ ] You opened your source's raw data and saw real values
- [ ] Your plan names a record key, at least two validation rules, and a cadence
- [ ] You can say in one sentence why someone would want this data

## How the workshop repo does it

- The [architecture doc](../ARCHITECTURE.md) is the workshop's plan, written up properly.
- The record key there is `symbol + trade_date` for daily bars and `symbol + bar_start_utc` for hourly bars. See the primary keys in [`sql/schema.sql`](../../sql/schema.sql).
- Its validation rules are listed in [`validation.py`](../../src/market_data/ingestion/validation.py).

## Stuck?

| Problem | What to do |
| --- | --- |
| The API asks for a key | Most free keys are fine. Keep the key out of your code (Lab 2 shows how). If it costs money, choose another source |
| The data has no obvious key | Ask: "If I loaded this twice, how would I know two rows are the same record?" Those columns are the key |
| The source only gives today's value, not history | That works. Your pipeline builds the history by running every day. Mention it in your plan |
| The dataset is huge | Choose one region, one product or one category. A pipeline that handles 1,000 rows well beats one that half-handles a million |

## Go further

- Read Section 1 of [Schema drift](../SCHEMA_DRIFT.md#1-what-can-drift) and add one more thing that could go wrong to your plan.
