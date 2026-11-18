# AI prompts for the labs

Copy-and-fill prompts that help you go faster with an AI assistant (ChatGPT, Claude, Gemini, Copilot, Cursor, or any other) while you build **your own** pipeline. Each lab links to its prompts here.

[All labs](README.md)

AI is good at the slow, mechanical parts: turning a sample response into a table definition, drafting tests, explaining an error. It is not a substitute for understanding. You will be asked to explain your pipeline, so every prompt below ends by asking the assistant to explain what it made.

## Rules for using AI in the labs

1. **You must be able to explain every line you commit.** If you can't, ask the assistant to explain it ([Explain a concept](#explain-a-concept)), or rewrite it more simply.
2. **Never paste secrets.** No passwords, API keys, Prefect keys, database URLs or `.env` contents. Replace them with `XXXX` before you paste anything, logs included.
3. **Give it real context.** Paste a real *sample* of your data (5–10 records, no personal data), your file, and the full error. Vague prompts get generic code.
4. **Tell it the versions.** Assistants often write code for old Prefect 2 or old Polars. The [project context](#start-here-your-project-context) block fixes that.
5. **Run it before you trust it.** Run `uv run ruff check .` and `uv run pytest` after every change. If the assistant says something works, the tests decide.
6. **Keep the worked example as your reference.** If the AI's answer looks very different from the lab's example, ask it why.

> [!TIP]
> In every prompt, replace each `[SQUARE BRACKET]` with your own details. Delete any line that doesn't apply.

## Start here: your project context

Paste this first in every new chat, filled in once from your [Lab 1 plan](lab-01-plan.md). It stops the assistant from guessing.

```text
I'm a student building my own batch data pipeline in a data engineering course.

My stack (please use exactly these, and their current APIs):
- Python 3.12, managed with uv (uv add, uv run). Project created with `uv init --package`, code in src/[PACKAGE_NAME]/
- Prefect 3 (flows and tasks; NOT Prefect 2 syntax), polars 2, httpx, python-dotenv, pytest, ruff
- Local database: SQLite. Production database: PostgreSQL on Supabase, through psycopg 3
- Medallion layers: Bronze = raw response saved as-is with metadata; Silver = typed, validated table; Gold = features computed from Silver

My pipeline:
- Source: [SOURCE NAME AND URL]
- What one record is: [e.g. "one city on one day"]
- Record key (unique per record): [e.g. location + observed_on]
- Cadence: [e.g. daily, new data appears around 02:00 UTC]
- Silver fields and rules: [PASTE THE "Data contract" SECTION OF YOUR PLAN]

How I want you to help:
- Keep the code simple and readable; I'm learning. Prefer small functions.
- Don't add libraries I didn't list without asking me first.
- After any code, explain what each part does and why, in plain language.
- If something in my request is a bad idea, say so.
```

---

## Any lab

### Explain a concept

```text
Explain [CONCEPT, e.g. "idempotent loads" / "a watermark with overlap" / "Prefect work pools"] to me.

1. Start with a one-sentence definition.
2. Give an everyday analogy.
3. Show how it applies to MY pipeline (context above), with a tiny example.
4. Tell me the most common mistake people make with it.
5. Then ask me 3 short questions to check I understood. Wait for my answers and correct me.
```

### Debug an error

```text
I ran this command:
[COMMAND]

And got this error (last lines of the output, secrets removed):
[PASTE THE LAST 20–40 LINES]

The file involved:
[PASTE THE FILE, OR THE FUNCTION]

What I expected: [WHAT SHOULD HAVE HAPPENED]
What I already tried: [ANYTHING]

Explain what the error means, what the most likely cause is, and the smallest fix.
Don't rewrite the whole file. If you need more information, tell me which command to run to get it.
```

Check the lab's **Stuck?** table and the [lab troubleshooting page](troubleshooting.md) first. The answer is often there already.

### Review my code before I commit

```text
Review this file from my pipeline as a strict but kind senior data engineer:
[PASTE FILE]

Check for:
- bugs and edge cases (empty data, missing fields, duplicates, time zones)
- anything that would break if the pipeline ran twice
- secrets or credentials in the code
- names or logic that would confuse a classmate

List problems from most to least serious, with a one-line fix for each. Don't rewrite the file.
```

### Prepare to explain my pipeline

Use this before class, or before you present.

```text
I need to explain my pipeline to my class. Here is my code:
[PASTE flow.py, AND ANY OTHER FILE YOU WANT TO COVER]

Act as my instructor. Ask me one question at a time about how and why it works
(the layers, the record key, what happens if it runs twice, what happens if the source is down,
how it is deployed). Wait for my answer each time, tell me what was right and what was missing,
then ask the next question. After 8 questions, summarise what I should revise.
```

---

## Lab 0 · Setup

```text
I'm setting up my machine for a Python data engineering course. I'm on [macOS / Windows 11 / Ubuntu].

I ran: [COMMAND]
And got: [OUTPUT]

Explain what went wrong and give me the exact commands to fix it for my operating system.
I need: Git, uv, Python 3.12 (installed through uv), VS Code, and the GitHub CLI (gh), logged in.
```

## Lab 1 · Choose your data and plan

### Find a source

```text
Suggest 5 free public data sources for a beginner batch data pipeline about [YOUR INTEREST, e.g. football / air quality / exchange rates / Ghana].

For each, give:
- the URL of the API docs, and whether it needs an API key
- what one record would be, and a natural record key
- how often new data appears, and whether old values get revised
- one example request I can try with curl

Only suggest sources that really exist and are free. Mark anything you are unsure about.
```

Then try the curl command yourself. If it doesn't return data, the source isn't real or has changed.

### Check my plan

```text
Here is a sample of the raw data from my source (from curl, trimmed to a few records):
[PASTE SAMPLE]

And here is my pipeline plan:
[PASTE YOUR PLAN]

Review the plan:
1. Is my record key really unique? Show me a case that could break it.
2. Which fields should be required in Silver, and what rules (ranges, formats) make sense for each?
3. What could go wrong with this source (rate limits, revisions, time zones, missing days)?
4. Is my cadence right for how often the data changes?
Don't write code yet.
```

## Lab 2 · Create the project

```text
Here is my project's pyproject.toml and the output of `tree -a -I .venv -L 3` (or `ls -R`):
[PASTE BOTH]

Explain, file by file, what each file and folder is for, and which ones must never be committed to Git.
Then check my .gitignore:
[PASTE .gitignore]
Does it keep out .env, data files and the virtual environment?
```

## Lab 3 · Hello-world pipeline

```text
Here is my first Prefect 3 flow:
[PASTE hello.py]

1. Explain the difference between a flow, a task, a flow run and a task run, using my code.
2. Explain what `retries` and `retry_delay_seconds` do, and what I would see in the logs.
3. Suggest one more small task I could add to practise passing data between tasks,
   in Prefect 3 syntax, without new libraries.
```

## Lab 4 · Deploy hello world

```text
Here is my prefect.yaml for a Prefect 3 deployment on a Prefect Managed work pool:
[PASTE prefect.yaml]

Explain it line by line: what happens, in order, when Prefect Cloud starts a run of this deployment?
Where does the code come from, which folder does it run in, and how does it find my flow?
Then tell me what would break if I forgot to `git push` before running it.
```

## Lab 5 · Extract into Bronze

### Draft the extract step

```text
[PASTE THE PROJECT CONTEXT BLOCK FIRST]

Here is a real response from my source, from this request:
[THE URL OR CURL COMMAND, WITH ANY API KEY REPLACED BY XXXX]
[PASTE A TRIMMED SAMPLE OF THE RESPONSE]

Write src/[PACKAGE_NAME]/extract.py with:
1. `fetch_[THING](...)` using httpx with a 30-second timeout. It returns the parsed response without cleaning it.
2. A `TemporarySourceError` exception, raised for timeouts, connection errors, HTTP 429 and 5xx.
   Any other HTTP error should raise normally (raise_for_status).
3. `save_bronze(payload, ...)` that writes the response as JSON under
   data/bronze/ingest_date=YYYY-MM-DD/[ANY EXTRA FOLDER, e.g. location=accra]/<uuid>.json, adding `_source` and `_ingested_at` (UTC ISO time).
[IF YOUR SOURCE NEEDS A KEY: Read the API key from the environment with os.getenv("[KEY_NAME]") in config.py. Never hard-code it.]
[IF YOUR SOURCE HAS PAGES: Follow the pagination until there are no more pages, and say how you detect the last page.]

Then write tests/test_extract.py with two pytest tests that don't use the internet
(replace httpx.get with a fake using monkeypatch): Bronze keeps the payload, and a 503 raises TemporarySourceError.
Explain why the two kinds of errors are treated differently.
```

Compare the result with the [Lab 5 worked example](lab-05-extract-bronze.md#2-write-the-extract-step). It should look similar.

## Lab 6 · Validate into Silver

### Draft the data contract and tests

```text
[PASTE THE PROJECT CONTEXT BLOCK FIRST]

Here is one of my Bronze files (trimmed):
[PASTE BRONZE JSON]

My Silver table should have these columns (my name ← source name, type):
[e.g. observed_on ← daily.time, Date
      temp_max_c ← daily.temperature_2m_max, Float64]

And these rules:
[e.g. no nulls in any column; no duplicate (location, observed_on); temp_max_c >= temp_min_c; precipitation_mm >= 0]

Write src/[PACKAGE_NAME]/validate.py with polars 2:
- a FIELDS dict mapping source names to my names
- `validate_[THING](payload, ...) -> pl.DataFrame` that fails fast with a ValueError naming the problem:
  first missing fields, then casts with strict=False and rejects nulls, then each rule
- the rules kept in one dict of {message: polars expression}, so they're easy to read

Then write tests/test_validate.py: one test for a good batch, one per rule
(each test breaks exactly one rule, using pytest.mark.parametrize), and one for a missing field.
Explain why we reject the whole batch instead of dropping bad rows.
```

> [!IMPORTANT]
> Read each rule the assistant wrote and ask yourself: *is this really true for my data?* A wrong rule either lets bad data in or rejects good data every day.

## Lab 7 · Gold and an idempotent load

### Choose Gold features

```text
My Silver table has these columns: [LIST COLUMNS AND TYPES], one row per [RECORD].
Who would use this data: [e.g. a farmer planning irrigation / a trader / me, for a dashboard].

Suggest 5 Gold features I could compute from Silver alone (changes, rolling averages, flags, categories).
For each, give the polars 2 expression, what it means in one sentence, and one edge case
(first row, gaps in dates, several locations). Mark the 2 most useful.
```

### Write the tables and the upserts

```text
[PASTE THE PROJECT CONTEXT BLOCK FIRST]

My Silver DataFrame has these columns and polars types:
[PASTE THE OUTPUT OF print(silver.schema)]
My Gold DataFrame has:
[PASTE THE OUTPUT OF print(gold.schema)]
The record key is: [COLUMNS]

Write, for SQLite:
1. CREATE TABLE IF NOT EXISTS statements for both tables, with the record key as the PRIMARY KEY
   and NOT NULL on every column that can't be empty. Add a loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP to Silver.
2. An INSERT ... ON CONFLICT (key) DO UPDATE SET ... statement for each table,
   updating every non-key column from `excluded`.
3. A `save(silver, gold)` function that writes both in ONE transaction.

Explain why running it twice with the same data doesn't create duplicates,
and what happens to Silver if the Gold insert fails.
```

Then prove it yourself: load the same batch twice and check the row counts don't change.

## Lab 8 · Orchestrate it

### Choose the watermark, overlap and schedule

```text
My source: [NAME]. New data appears [WHEN, AND IN WHICH TIME ZONE].
It [never revises / revises the last N days] of data.
My record key is [KEY], and my table's latest date is found with [e.g. SELECT max(observed_on)].

Help me choose:
1. the overlap (how far back each run should re-fetch), and why
2. what "end" should be for a normal run (today? yesterday? the last complete hour?)
3. a cron schedule (with the time zone) that runs after the new data is available
Explain what happens if the pipeline misses three runs, with your choices.
```

### Draft the flow

```text
[PASTE THE PROJECT CONTEXT BLOCK FIRST]

Here are my functions (signatures and docstrings only):
[PASTE extract.py, validate.py, transform.py and load.py SIGNATURES]

Write src/[PACKAGE_NAME]/flow.py with Prefect 3:
- one task per step; the fetch task retries 3 times with growing delays [e.g. 5, 30, 60 s],
  but ONLY for TemporarySourceError (use retry_condition_fn)
- a flow `[NAME]_pipeline(start: date | None = None, end: date | None = None)`:
  with no dates, it starts from the latest stored date minus [N] days of overlap (or [FIRST DATE] on an empty table);
  with dates, it backfills exactly that range; it refuses start > end
- a `main()` with argparse for --start, --end and --serve (serve on cron "[CRON]")

Then write 2 tests using prefect_test_harness: an end-to-end run with a faked fetch, and a retry
(the fake fails once with TemporarySourceError, then succeeds).
Explain how the watermark makes a missed run catch up by itself.
```

## Lab 9 · Go to production

### Create your Supabase tables

```text
Here are my SQLite tables from load.py:
[PASTE YOUR TABLES SQL]

Rewrite them for PostgreSQL on Supabase, to paste into the Supabase SQL Editor:
- keep the same table and column names and the same PRIMARY KEY
- use proper PostgreSQL types (DATE, TIMESTAMP, DOUBLE PRECISION, BOOLEAN, TEXT, BIGINT)
- keep CREATE TABLE IF NOT EXISTS
- add ALTER TABLE ... ENABLE ROW LEVEL SECURITY for each table (no policies)

Then list any differences between my SQLite and PostgreSQL versions that could change
how my upserts behave (types, placeholders, booleans, dates).
Explain what Row Level Security does here and why my pipeline can still write.
```

Paste the result into **SQL Editor** as in [Lab 9 Part A2](lab-09-production.md#a2-create-your-tables), and check the tables appear in **Table Editor**.

### Write SQL to check your data

```text
My Supabase (PostgreSQL) tables are:
[PASTE THE CREATE TABLE STATEMENTS]

Write read-only SQL queries I can run in the Supabase SQL Editor to:
1. count rows per [LOCATION / CATEGORY] and show the first and last date
2. find gaps: dates with no row, between the first and last date
3. find duplicates on [RECORD KEY] (there should be none)
4. show the 10 most recently loaded rows
5. [YOUR OWN QUESTION, e.g. the 5 rainiest days]
Use only SELECT. Explain each query in one sentence.
```

> [!CAUTION]
> Read any SQL before you run it on your production database. Run only `SELECT` queries you understand. Anything with `DELETE`, `DROP`, `TRUNCATE` or `UPDATE` changes your data, and the SQL Editor doesn't ask twice.

### Make load.py work with both databases

```text
Here is my load.py, which works with SQLite:
[PASTE load.py]

Change `open_database` so it also works with PostgreSQL through psycopg 3, when given a
postgresql:// URL, while SQLite stays the default for local runs and tests.
SQLite uses "?" placeholders and psycopg uses "%s": keep the SQL in one place, with the placeholder filled in.
Keep both writes in one transaction. Don't add an ORM. Explain each change.
```

### Review my deployment and CI

```text
Here are my prefect.yaml and .github/workflows/ci.yml:
[PASTE BOTH]
And the output of `uv tree --depth 1 --no-dev`:
[PASTE]

Check:
1. Every package my code imports is in pip_packages, with the same version as uv.lock.
2. The deploy job only runs on pushes to main, after the checks job passes.
3. No secret is written in either file (they must come from GitHub secrets or a Prefect Secret block).
4. The deployment runs the exact commit that was tested.
List problems only, most serious first.
```

## Lab 10 · Harden and hand over

### Drift and dependencies

```text
My source used to send these fields: [LIST]. Today it sends: [LIST].
Here is my validate.py:
[PASTE]

1. Which changes would my pipeline reject, which would it silently ignore, and which would break it in a confusing way?
2. For each, should I fail fast, rescue the field, or evolve my contract? Why?
```

```text
`uv audit` reported this (trimmed):
[PASTE THE FINDING]

Explain in plain language: what the vulnerability is, whether my pipeline is likely to be affected
(it [describe: calls one public API, writes to Postgres, no web server]),
and the safest fix with uv (uv lock --upgrade-package ...). Don't suggest ignoring it.
```

### Draft the runbook

```text
Here is my README, prefect.yaml and flow.py:
[PASTE]

Draft a "Runbook" section for my README for someone who has never seen this project:
what it does, the schedule, how releases happen, and a table of how to: see runs and logs,
run now, backfill a date range, pause the schedule, rotate the database password, run locally.
Then a table of the 3 most likely failures, what they mean and what to do.
Use only commands that appear in my files. Mark anything you had to guess with TODO.
```

Then give it to a classmate, as [Lab 10 Part D](lab-10-harden.md#part-d--write-the-runbook) says. Fix every TODO before you merge.

---

## When the AI is wrong

It will be, sometimes. Typical signs, and what to do:

| Sign | Likely cause | Do this |
| --- | --- | --- |
| `Deployment.build_from_flow`, `prefect deployment build`, `prefect agent start` | Prefect 2 code | Remind it: *"Use Prefect 3 only."* Compare with the lab's example |
| `pl.read_json(...).apply(...)`, `with_column`, `groupby` | Old Polars names | Remind it: *"Use polars 2 (`map_elements`, `with_columns`, `group_by`)."* |
| `pip install …` or `requirements.txt` | It doesn't know you use uv | `uv add PACKAGE` instead |
| An API URL or field that doesn't exist | It made it up | Check the source's docs and try the URL with curl |
| Tests that pass but test nothing | The test only checks its own fake | Break your code on purpose. The test should fail |
| A password or key written into the code | It took a shortcut | Move it to `.env` (local) or a Secret block (cloud) |
