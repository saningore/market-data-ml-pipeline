# Lab troubleshooting

Problems students hit in the labs, grouped by lab. Each lab's **Stuck?** table covers the most common ones. This page has more. For the workshop repository itself, see the main [troubleshooting guide](../TROUBLESHOOTING.md).

[All labs](README.md)

## Before anything else

1. **Read the error from the bottom up.** In Python, the last line names the exception and the message. The lines above it show where it happened. Start with the last line that mentions *your* file.
2. **Check where you are.** Most "file not found" and "module not found" errors come from running a command in the wrong folder. Run `pwd` (PowerShell: `Get-Location`). You should be in the folder that contains `pyproject.toml`.
3. **Check which Prefect you're talking to.** `uv run prefect config view` shows `PREFECT_API_URL` and where it came from.
4. **Re-sync.** `uv sync` makes `.venv` match `uv.lock` again.
5. **Ask well.** Post the command, the last 20 lines of output, and what you expected. Never post passwords, API keys or database URLs.

## Lab 0 · Setup

| Symptom | Cause | Fix |
| --- | --- | --- |
| `command not found` (or `is not recognized` on Windows) right after an install | The terminal started before the install changed `PATH` | Close every terminal window and open a new one |
| `uv python install` hangs or fails | Network or proxy blocks the download | Try another network. Behind a proxy, set `HTTPS_PROXY` as your network admin tells you |
| `xcode-select: error: command line tools are already installed` | Git is already there | Nothing to do. Run `git --version` |
| `gh auth status` says you aren't logged in | The login didn't finish | Run `gh auth login` again and complete the browser step |
| Git asks for a password on `git push` | Git isn't using the GitHub CLI login | Run `gh auth setup-git` |

## Lab 2 · Project

| Symptom | Cause | Fix |
| --- | --- | --- |
| `error: No pyproject.toml found` | You're not in the project folder | `cd my-pipeline` |
| `uv add` fails with `No solution found` | A package doesn't support your Python version | Check `.python-version` says `3.12`, then run `uv python install 3.12` |
| `ruff format --check` fails | Files aren't formatted | Run `uv run ruff format .`, then commit |
| `ruff check` reports `I001` | Imports aren't sorted | Run `uv run ruff check --fix .` |
| `.venv` or `data/` shows in `git status` | `.gitignore` is missing lines, or the files were committed before the rule existed | Fix `.gitignore`, then `git rm -r --cached .venv data` and commit |
| VS Code underlines every import in red | VS Code uses a different Python | **Python: Select Interpreter** → the one in `.venv` |
| `gh repo create` fails with `HTTP 403` | The GitHub CLI token is missing a scope | `gh auth refresh -s repo` |

## Lab 3 · Prefect locally

| Symptom | Cause | Fix |
| --- | --- | --- |
| `RuntimeError: Failed to reach API at http://127.0.0.1:4200/api/` | The active profile points at a local server that isn't running | Start it with `uv run prefect server start`, or `uv run prefect profile use default` |
| `Address already in use` on port 4200 | Another server is running | Find and close the other terminal. Or use `--port 4201` and `uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4201/api` |
| The UI is blank or says it can't connect | The browser reaches a different API than the server | Open exactly `http://127.0.0.1:4200`, and restart the server after changing profiles |
| Runs appear in Prefect Cloud instead of locally | The cloud profile is active | `uv run prefect profile use local` |
| `prefect profile create local` says it exists | You made it before | Skip that line and run `prefect profile use local` |
| Tests hang or try to reach port 4200 | `tests/conftest.py` with `prefect_test_harness` is missing or in the wrong folder | Put it in `tests/`, exactly as in Lab 3 |
| A task never retries | `retries` is missing from `@task(...)`, or the retry condition returns `False` | Check the decorator. Look for `Retrying` in the logs |

## Lab 4 · Prefect Cloud

| Symptom | Cause | Fix |
| --- | --- | --- |
| `prefect cloud login` shows no workspaces | The workspace wasn't created | Create one at [app.prefect.cloud](https://app.prefect.cloud), then log in again |
| `Work pool 'my-managed-pool' does not exist` | The pool name in `prefect.yaml` doesn't match | `uv run prefect work-pool ls`, then fix the name |
| `prefect deploy` asks questions and waits | `--no-prompt` is missing, or `prefect.yaml` is incomplete | Add `--no-prompt`. Read any error it prints about missing fields |
| `FileNotFoundError` for the entrypoint during `prefect deploy` | You ran it from the project root | `cd src`, then `uv run prefect deploy --prefect-file ../prefect.yaml …` |
| The cloud run fails in `git_clone` | Wrong URL, a private repository, or the branch doesn't exist | Open the URL while signed out. Check that `branch:` exists on GitHub. Private repositories need a token ([Lab 4 Stuck?](lab-04-deploy-hello.md#stuck)) |
| The cloud run uses old code | You didn't push, or the deployment is pinned to an older commit | `git push`, and run again. In Lab 9 and later, merge to `main` so CI redeploys |
| The run waits in `Pending` for a long time | Free-tier Managed pools have limited capacity and run time | Wait a few minutes. Check **Work Pools** for a paused pool, and your account's usage page |

## Labs 5–7 · Extract, validate, load

| Symptom | Cause | Fix |
| --- | --- | --- |
| `httpx.ReadTimeout` | The API is slow | It's a temporary error, so Lab 8's retries handle it. For manual tests, try again or fetch a smaller range |
| `JSONDecodeError` | The API returned HTML or an error page, not JSON | Print `response.status_code` and `response.text[:500]` |
| Dates are off by one day | Time zones | Ask the API for UTC (`timezone=UTC` in the example) and keep dates in UTC everywhere |
| `polars.exceptions.ComputeError: could not find an appropriate format` | `str.to_date` can't guess the date format | Give the format explicitly, such as `"%d/%m/%Y"`, with `strict=False` |
| `InvalidOperationError: conversion … failed` | A cast without `strict=False` met a bad value | Use `strict=False` and let your null rule report it |
| Validation passes but values look wrong | A rule is missing | Add the rule *and* a test that breaks only that rule |
| `sqlite3.OperationalError: no such table` | The tables weren't created | Make sure `open_database` runs `TABLES` |
| `sqlite3.OperationalError: table … has no column named …` | You changed the table, but an old database file has the old shape | Delete `data/pipeline.sqlite3` (it's rebuilt from the source) or `ALTER TABLE` it |
| `DeprecationWarning: The default date adapter is deprecated` | SQLite got a Python `date` without an adapter | Keep the `sqlite3.register_adapter(date, date.isoformat)` line from Lab 7 |
| Row counts grow on every run | The load isn't idempotent | The primary key must be your record key, and the insert must use `ON CONFLICT … DO UPDATE` |

## Lab 8 · Orchestration

| Symptom | Cause | Fix |
| --- | --- | --- |
| `uv run my-pipeline` runs the old hello `main` | `[project.scripts]` still points to `my_pipeline:main` | Change it to `my_pipeline.flow:main`. `uv run` reinstalls the project automatically |
| `error: argument --start: invalid fromisoformat value` | The date isn't `YYYY-MM-DD` | Use, for example, `--start 2026-09-01` |
| The catch-up window is wrong | The overlap is in days, but the source updates hourly (or the other way round) | Make `OVERLAP_DAYS` (or hours) match how far back your source revises |
| Retries happen for validation errors | The retry condition is too broad | Retry only your `TemporarySourceError` |
| `serve` exits right away | Another error happened during start-up. The last lines say which | Usually the server isn't running. Start it first |
| The scheduled run didn't happen | The laptop slept, or the `serve` terminal was closed | Expected for a laptop. Lab 9 moves the schedule to the cloud |
| Tests are slow | Real retry delays | Use `fetch.with_options(retry_delay_seconds=0)` in tests, as in Lab 8 |

## Lab 9 · Production

| Symptom | Cause | Fix |
| --- | --- | --- |
| `psycopg.OperationalError: … password authentication failed` | Wrong password, or special characters not encoded | Percent-encode it (`@` → `%40`). Or reset the database password in Supabase **Settings → Database** |
| `… Network is unreachable` or `timeout expired` | Direct connection string on an IPv4-only network | Use the **Session pooler** string |
| `prepared statement … already exists` | You used the **Transaction** pooler (port 6543) | Use the **Session** pooler (port 5432) |
| `permission denied for schema public` | Your tables are in a schema the user can't write to | Create the tables in SQL Editor as shown in A2, and connect with the URL from **Connect** |
| A Supabase advisor warns *RLS disabled* | Row Level Security is off on a public table | Run the two `ALTER TABLE … ENABLE ROW LEVEL SECURITY` lines from A2 |
| `ModuleNotFoundError` in the cloud run, but not locally | `pip_packages` is missing a package | Add every package your code imports, with the version from `uv tree --depth 1 --no-dev` |
| `ImportError` or odd behaviour only in the cloud | `pip_packages` versions differ from `uv.lock` | Make them equal. Copy the idea of `tests/test_deployment.py` from the workshop repo to enforce it |
| The CI `checks` job fails but tests pass locally | Formatting, the lock file, or a test that needs your `.env` | Run the exact CI commands: `uv sync --locked`, `uv run --locked ruff format --check .`, `uv run --locked pytest`. If `--locked` fails, run `uv lock` and commit `uv.lock` |
| `uv audit` fails in CI | A dependency has a known vulnerability | [Lab 10 Part B](lab-10-harden.md#part-b--keep-dependencies-safe) |
| The `deploy` job prints `PREFECT_API_URL` errors | The variable is missing or misspelled | `gh variable list` should show `PREFECT_API_URL` |
| `Required status check "checks" is expected` and the merge is blocked | The ruleset waits for a check that hasn't run yet | Push a commit to the PR branch so the workflow runs, or check the `on:` section includes `pull_request` |
| You locked yourself out of `main` | That's the ruleset working | Make every change through a PR. To change the ruleset itself, use **Settings → Rules** |

## Lab 10 · Hardening

| Symptom | Cause | Fix |
| --- | --- | --- |
| `uv audit` says `unknown preview feature` | An old uv | `uv self update` (CI pins its own version) |
| An upgrade breaks a test | The new version changed behaviour | Read the package's changelog, fix your code or pin the last safe version, and note it in the PR |
| Too many alert emails | The trigger matches every flow, or every state | Scope it to your flow and to `Failed` and `Crashed` only |
| A classmate can't follow the runbook | It assumes knowledge they don't have | Add the missing step. That's what the test is for |
