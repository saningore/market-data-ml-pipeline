# Lab 3 · Hello-world pipeline

**Time:** 45 minutes · **You will:** build a three-step pipeline with Prefect, watch it in the Prefect UI, and see a retry rescue a failure.

[All labs](README.md) · Previous: [Lab 2](lab-02-project.md) · Next: [Lab 4 · Deploy hello world](lab-04-deploy-hello.md)

## Why it matters

Every pipeline does the same three things: **extract** data, **transform** it and **load** it somewhere. On a laptop you could call three functions in a row. In production you also need to know which step failed, retry the step that can recover, keep the logs, and run the whole thing on a schedule. That is **orchestration**, and Prefect does it for you.

Four Prefect words you'll use from now on:

| Word | Meaning |
| --- | --- |
| **Task** | One step, a Python function marked with `@task` |
| **Flow** | The function that calls the tasks in order, marked with `@flow` |
| **Run** | One execution of a flow or a task, with its own logs |
| **State** | Where a run is: `Scheduled`, `Running`, `Completed`, `Failed`, `Retrying` and so on |

## Steps

### 1. Write the pipeline

Create `src/my_pipeline/hello.py`:

```python
"""A hello-world pipeline: three tiny tasks wired together by a flow."""

from prefect import flow, task
from prefect.runtime import task_run


@task(retries=2, retry_delay_seconds=3)
def extract() -> list[int]:
    """Pretend to read data from a source that fails the first time we ask."""
    if task_run.run_count == 1:
        raise ConnectionError("The source didn't answer. Prefect will retry.")
    return [1, 2, 3]


@task
def transform(numbers: list[int]) -> list[int]:
    """Change the data: here, multiply every number by 10."""
    return [number * 10 for number in numbers]


@task
def load(numbers: list[int]) -> None:
    """Pretend to save the data somewhere."""
    print(f"Loaded {len(numbers)} numbers: {numbers}")


@flow(log_prints=True)
def hello_pipeline(name: str = "world") -> list[int]:
    """Run extract, transform and load in order."""
    print(f"Hello, {name}! Starting the pipeline.")
    numbers = extract()
    bigger = transform(numbers)
    load(bigger)
    return bigger


if __name__ == "__main__":
    hello_pipeline()
```

Read it before you run it:

- `extract` fails on purpose the first time (`run_count == 1`), to imitate a source that times out. `retries=2` lets Prefect try twice more.
- `log_prints=True` sends every `print` to Prefect's logs.
- The flow passes each task's result to the next task, like normal Python.

### 2. Run it

```sh
uv run python src/my_pipeline/hello.py
```

The output ends like this (your run names differ, because Prefect invents them):

```text
Starting temporary server on http://127.0.0.1:8998
Flow run 'maize-salmon' - Hello, world! Starting the pipeline.
Task run 'extract-e1f' - Task run failed with exception: ConnectionError("The source didn't answer. Prefect will retry.") - Retry 1/2 will start 3 second(s) from now
Task run 'load-350' - Loaded 3 numbers: [10, 20, 30]
Flow run 'maize-salmon' - Finished in state Completed()
```

The first attempt failed, Prefect waited 3 seconds and retried, and the flow still **completed**. Prefect started a *temporary* server for this run and stopped it at the end. Next you'll run a permanent one with a UI.

### 3. Start the Prefect UI

Prefect keeps its settings in **profiles**. Make a `local` profile that points to a server on your own machine. Profiles belong to your user account, not the project, so you do this once.

**Terminal 1** (leave it running):

```sh
uv run prefect profile create local
uv run prefect profile use local
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
uv run prefect server start
```

`profile use` may print `Error connecting to Prefect server`. That's expected, because the server isn't running yet. Wait until `server start` prints the address, then open [http://127.0.0.1:4200](http://127.0.0.1:4200).

**Terminal 2.** Open a second terminal in the same project folder:

```sh
uv run python src/my_pipeline/hello.py
```

In the UI, open **Runs** and click your run. Explore:

- the **graph**, which shows the three tasks and the order they ran in
- the **logs**, including the failed first attempt of `extract`
- each task's **state**, where `extract` went `Running → Retrying → Completed`

### 4. Pass a parameter

Flows take parameters like normal functions:

```sh
uv run python -c "from my_pipeline.hello import hello_pipeline; hello_pipeline(name='Ama')"
```

In the UI, open this run and look at its **Parameters** tab.

### 5. Break it on purpose

Change `retries=2, retry_delay_seconds=3` to `retries=0` and run the file again. Now the flow ends in `Failed`, and `transform` and `load` never run. Open the run in the UI and find the error message in the logs. Then put the retries back.

**The lesson:** retries are for problems that go away by themselves, like a timeout. A bug in your code fails every time, so retrying it only wastes time. Lab 8 retries *only* the errors worth retrying.

### 6. Test it

A test should never depend on a server you forgot to start. Create `tests/conftest.py`, which pytest loads automatically:

```python
"""Shared test setup."""

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_for_tests():
    """Run every flow in the tests against a throwaway Prefect database."""
    with prefect_test_harness():
        yield
```

Create `tests/test_hello.py`:

```python
from my_pipeline.hello import hello_pipeline


def test_hello_pipeline_multiplies_by_ten():
    assert hello_pipeline(name="test") == [10, 20, 30]
```

Run the checks:

```sh
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

### Your turn

Copy `hello.py` to `src/my_pipeline/plan_sketch.py`, then rename the tasks and messages to match **your** plan from Lab 1. For example, a currency pipeline might have `fetch_rates`, `check_rates` and `save_rates`, each printing what it *will* do. Run it and look at the graph in the UI. You've just drawn your real pipeline before writing it.

Commit:

```sh
git add .
git commit -m "feat: add a hello-world Prefect pipeline"
git push
```

## Checkpoint

- [ ] The Prefect UI at `http://127.0.0.1:4200` shows at least three runs of `hello-pipeline`
- [ ] You can find the failed first attempt of `extract` in a run's logs
- [ ] `uv run pytest` passes even when the Prefect server is stopped

## How the workshop repo does it

- [`historical_ingestion.py`](../../src/market_data/flows/historical_ingestion.py) is the same pattern at full size: a flow calling `fetch_prices`, `save_bronze`, `validate_and_transform` and `store_market_data`.
- Its fetch task can simulate a timeout too. See `demo_failure` in that file.
- Its tests use the same idea with a temporary Prefect server. See [`tests/test_historical_ingestion.py`](../../tests/test_historical_ingestion.py).
- The README section [Prefect UI and local daily schedule](../../README.md#prefect-ui-and-local-daily-schedule) uses terminal variables instead of a profile. Both work.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `RuntimeError: Failed to reach API at http://127.0.0.1:4200/api/` | Your profile points at the local server, but it isn't running. Start it in Terminal 1, or switch profiles with `uv run prefect profile use default` |
| `Port 4200 is already in use` | Another Prefect server is running, maybe in a forgotten terminal. Close it, or start this one with `--port 4201` and set `PREFECT_API_URL` to match |
| The UI opens but shows no runs | The run went to a different server. Run `uv run prefect config view` in Terminal 2 and check that `PREFECT_API_URL` is `http://127.0.0.1:4200/api` |
| `ModuleNotFoundError: No module named 'my_pipeline'` | Run commands from the project folder (the one with `pyproject.toml`) and with `uv run` in front |

More fixes: [lab troubleshooting](troubleshooting.md#lab-3--prefect-locally).

## Go further

- [Prefect: write and run a workflow](https://www.prefect.io/docs/v3/how-to-guides/workflows/write-and-run)
- [Prefect: retries](https://www.prefect.io/docs/v3/how-to-guides/workflows/retries)
