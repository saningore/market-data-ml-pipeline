# Lab 10 · Harden and hand over

**Time:** 75 minutes · **You will:** make your pipeline notice when the source changes shape, keep its dependencies safe, alert you when it fails, and write the runbook someone else would need to look after it.

[All labs](README.md) · Previous: [Lab 9](lab-09-production.md) · Next: the [assignment](../STUDENT_ASSIGNMENT.md#part-2--build-your-own-pipeline), and the MLOps labs (coming)

## Why it matters

A pipeline that works today will meet three problems:

1. **The source changes shape.** A field is added, renamed or removed. This is called *schema drift*.
2. **A dependency gets a security hole.**
3. **It fails at 06:30 on a Sunday, and nobody looks.**

The last step of building a pipeline is making sure these problems are noticed and can be fixed, by you or by someone who has never seen your code.

---

## Part A · Notice schema drift

[Schema drift](../SCHEMA_DRIFT.md) describes four responses. Your pipeline already does two of them:

| Change at the source | Response | Your pipeline |
| --- | --- | --- |
| A **required** field disappears or is renamed | **Fail fast**: reject the batch | ✅ Lab 6: `missing fields: …` |
| A row breaks a rule | **Fail fast** (or quarantine the row) | ✅ Lab 6: the rule names the problem |
| A **new** field appears | **Rescue** it, or at least *notice* it | ❌ Silently ignored. You add this now |
| You *want* the new field | **Evolve**: change the contract on purpose | You, in a reviewed pull request |

### A1. Notice new fields

Add this function at the end of `src/my_pipeline/validate.py`:

```python
def unexpected_fields(payload: dict) -> list[str]:
    """List fields the source sent that the contract doesn't use: an early sign of drift."""
    daily = payload.get("daily")
    return sorted(daily.keys() - FIELDS.keys()) if isinstance(daily, dict) else []
```

In `src/my_pipeline/flow.py`, import it (`from my_pipeline.validate import unexpected_fields, validate_weather`) and make the `check` task log a **warning** when it finds any:

```python
@task
def check(payload: dict, location: str) -> pl.DataFrame:
    extra = unexpected_fields(payload)
    if extra:
        get_run_logger().warning(
            "Schema drift: the source sent new fields %s. Silver ignores them until you "
            "add them to the contract.",
            extra,
        )
    return validate_weather(payload, location)
```

Add a test to `tests/test_validate.py`, and add `unexpected_fields` to its import line:

```python
def test_new_source_fields_are_noticed_but_dont_break_the_batch():
    payload = make_payload(rain_sum=[0.0, 2.0, 0.0])
    assert unexpected_fields(payload) == ["rain_sum"]
    assert validate_weather(payload, "accra").height == 3
```

### A2. Simulate both kinds of drift

**A field is added.** In `extract.py`, add `"rain_sum"` to `DAILY_FIELDS`, as if the provider started sending it, and run `uv run my-pipeline`:

```text
WARNING | Task run 'check-e75' - Schema drift: the source sent new fields ['rain_sum']. Silver ignores them until you add them to the contract.
INFO    | Flow run 'organic-cockatrice' - Saved 4 Silver rows and refreshed 61 Gold rows.
```

The run still succeeds, and now you know. Undo the change.

**A field is renamed.** In `validate.py`, change `"precipitation_sum"` to `"precipitation_total"` in `FIELDS`, as if the provider renamed it, and run again:

```text
ERROR   | Task run 'check-9b0' - Task run failed with exception: ValueError('accra: missing fields: precipitation_total.')
```

The batch is rejected, Silver is untouched, and the error names the field. Undo the change.

> [!TIP]
> Want the full **rescue** pattern? Store the unknown fields as a JSON text column (for example `_rescued_data`) in Silver, so nothing is lost. The workshop's hourly pipeline does exactly that. See `rescue_extra_columns` in [`validation.py`](../../src/market_data/ingestion/validation.py) and Step 2 of [Schema drift](../SCHEMA_DRIFT.md#step-2--rescue-a-field-you-didnt-expect).

---

## Part B · Keep dependencies safe

Your CI already runs `uv audit` (Lab 9), which checks every locked package against public vulnerability databases. Run it locally too:

```sh
uv audit --locked --preview-features audit
```

When it reports a vulnerability:

1. **Read the advisory.** Which package, which versions, and what is the risk?
2. **Upgrade the package**, usually the fix: `uv lock --upgrade-package NAME`, then `uv sync` and `uv run pytest`.
3. If the package is also in `pip_packages` in `prefect.yaml`, **update the pin there too**.
4. Release the fix through a pull request, as in Lab 9.

Because the ruleset requires `checks`, a vulnerable dependency **blocks the merge** until it's fixed. That's on purpose. The [security guide §4](../SECURITY.md#when-the-audit-fails-triage-first) explains triage, and how to accept a finding that truly doesn't apply to you with a reviewed, expiring *waiver*.

Also switch on GitHub's free helpers. In **Settings → Advanced Security** (called **Code security** on some plans), turn on **Dependabot alerts** and **push protection**. Push protection blocks a push that contains an API key. [Security §3](../SECURITY.md#3-walk-through-githubs-security-features) shows where each one is.

---

## Part C · Get told when it fails

In [Prefect Cloud](https://app.prefect.cloud):

1. Open **Automations** and click **+**.
2. **Trigger:** choose **Flow run state**. Pick your flow `weather-pipeline` and the state **Failed** (add **Crashed** too).
3. **Action:** choose **Send a notification**, with an email block that sends to your own address. Create the block when asked.
4. Name it `weather pipeline failed` and click **Create**.

Test it with a run that fails on purpose, a backfill whose start comes after its end:

```sh
uv run prefect deployment run 'weather-pipeline/daily-weather' -p start=2026-11-10 -p end=2026-11-01
```

Within a few minutes, you should get an email.

---

## Part D · Write the runbook

A **runbook** is the page someone opens when the pipeline breaks. Add a section like this to your `README.md`, filled in for your pipeline:

```markdown
## Runbook

**What it does:** loads yesterday's weather for Accra from Open-Meteo into Supabase
(`weather_daily` for Silver, `weather_features` for Gold).
**Schedule:** every day at 06:30 UTC, Prefect deployment `weather-pipeline/daily-weather`.
**Releases:** merge a pull request into `main`; GitHub Actions tests and deploys it.

| Task | How |
| --- | --- |
| See recent runs and logs | Prefect Cloud → Runs, filter by `weather-pipeline` |
| Run now | `uv run prefect deployment run 'weather-pipeline/daily-weather'` |
| Backfill a range | `uv run prefect deployment run 'weather-pipeline/daily-weather' -p start=YYYY-MM-DD -p end=YYYY-MM-DD` |
| Pause the schedule | Toggle the schedule off on the deployment page, or `uv run prefect deployment schedule pause 'weather-pipeline/daily-weather' SCHEDULE_ID` (find the ID with `schedule ls`) |
| Rotate the database password | Change it in Supabase, then update the `my-pipeline-database` Secret block. No code change |
| Run locally | `cp .env.example .env`, `uv sync`, `uv run my-pipeline` (SQLite) |

**Common failures**

| Error in the logs | Meaning | Action |
| --- | --- | --- |
| `TemporarySourceError` after 3 retries | The API is down | Nothing; the next run catches up |
| `missing fields: …` | The source changed shape | Check Bronze/the API docs, update the contract in a PR |
| `password authentication failed` | The credential changed | Update the Secret block |

**Known limitations:** the 7-day average counts rows, not calendar days, so it spans gaps.
Bronze isn't kept in production.
```

The best test of a runbook: give it to a classmate, and ask them to backfill one week of your pipeline using only the README.

---

## Your turn

> [!TIP]
> **Using AI?** Copy-and-fill prompts for this lab are in [AI prompts](ai-prompts.md#lab-10--harden-and-hand-over). You still need to be able to explain every line you commit.

1. Add drift detection to **your** pipeline, and simulate an added and a renamed field.
2. Run `uv audit` locally, and turn on Dependabot alerts and push protection.
3. Create the failure automation and trigger it once.
4. Write your runbook, and have a classmate follow it.
5. Release all of it through a pull request.

## Final checklist

This also covers most of the [assignment rubric](../STUDENT_ASSIGNMENT.md#part-2--build-your-own-pipeline):

| Rubric line | Where you did it |
| --- | --- |
| Source and Bronze | Lab 5 |
| Validation | Lab 6, plus drift detection in Lab 10 |
| Transformation | Lab 6 (Silver) and Lab 7 (Gold) |
| Persistent store | Lab 7 (SQLite) and Lab 9 (PostgreSQL, Secret block) |
| Repeatability | Lab 7 (idempotent load) and Lab 8 (backfill) |
| Orchestration | Labs 3, 4, 8 and 9 |
| Documentation | Lab 10 runbook |
| Bonus: schema drift, CI, scheduled deployment | Labs 9 and 10 |

- [ ] A classmate cloned your repository and ran it locally using only the README
- [ ] `git ls-files` shows no `.env`, database file or credential
- [ ] Running the pipeline twice doesn't change the row counts
- [ ] The production deployment ran on schedule at least once without you
- [ ] Your README states one thing your pipeline can't handle yet

## How the workshop repo does it

- Rescue in hourly Silver: `rescue_extra_columns` in [`validation.py`](../../src/market_data/ingestion/validation.py)
- The audit gate with reviewed waivers: [`security/audit.py`](../../security/audit.py) and [Security §4](../SECURITY.md#4-make-the-security-check-block-the-merge)
- A scoped failure alert, demonstrated in class: [Instructor guide, cloud follow-up](../INSTRUCTOR_DEMO.md#cloud-follow-up-pr-schedule-and-failure-alert)
- Its runbook-style pages: [Troubleshooting](../TROUBLESHOOTING.md) and [Deployment §7](../DEPLOYMENT.md#7-collect-hourly-gold-bars)

## Stuck?

| Symptom | Fix |
| --- | --- |
| No drift warning appears | Warnings come from the `check` task. Check that it calls `unexpected_fields` and that the run uses your new code (in the cloud, that it's merged and deployed) |
| `uv audit` reports a package you don't use directly | It's a dependency of a dependency. Upgrading it with `uv lock --upgrade-package NAME` still works. Run the tests afterwards |
| No email from the automation | Check the email block's address and your spam folder. Check that the trigger names the right flow and the state **Failed** |
| `-p start=…` gives a validation error | Dates must be `YYYY-MM-DD`. Prefect checks parameter types before the run starts |

More fixes: [lab troubleshooting](troubleshooting.md#lab-10--hardening).

## What's next

You've built, deployed and hardened your own pipeline. The **MLOps labs** pick up from your Gold table. You'll define a prediction target, train and track models, and put one into production with the same CI/CD discipline. See [Coming next](README.md#coming-next-mlops-labs).
