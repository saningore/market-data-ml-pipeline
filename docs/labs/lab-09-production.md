# Lab 9 · Go to production

**Time:** about 2 hours · **You will:** load your data into a cloud database on a schedule, with no laptop involved. Every release is tested by CI and goes through a pull request.

[All labs](README.md) · Previous: [Lab 8](lab-08-orchestrate.md) · Next: [Lab 10 · Harden and hand over](lab-10-harden.md)

## Why it matters

Your pipeline works on your laptop. Production adds four things:

| Production needs | Laptop version | Production version | Part |
| --- | --- | --- | --- |
| A database others can reach | SQLite file | PostgreSQL on Supabase | A |
| Credentials that never touch Git | `.env` | Prefect **Secret block** | B |
| Runs that don't need you | `serve` in a terminal | Prefect **Managed** work pool on a schedule | C |
| Only tested code reaches production | You run `pytest` | **CI** tests every change, and only `main` deploys | D, E |

This is the production path you're building:

```mermaid
flowchart LR
    A["Work on dev"] --> B["Pull request into main"]
    B --> C["GitHub Actions runs the checks"]
    C --> D["Merge (only if checks pass)"]
    D --> E["Actions registers that commit in Prefect Cloud"]
    E --> F["On schedule, a Managed container clones that commit"]
    F --> G["It reads the database URL from a Secret block"]
    G --> H["It loads Silver and Gold into Supabase"]
```

Take it one part at a time, and check each part before you start the next.

---

## Part A · A database in the cloud

### A1. Create a Supabase project

1. Sign in at [supabase.com/dashboard](https://supabase.com/dashboard) and click **New project**.
2. Choose a name and a region near you, and set a **database password**. Save the password in a password manager. You'll need it in A3.
3. Click **Create new project** and wait a minute or two.

### A2. Create your tables

Open **SQL Editor → New query**. Paste your `TABLES` SQL from `load.py`, plus two lines that switch on Row Level Security, and click **Run**. For the worked example:

```sql
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
ALTER TABLE weather_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE weather_features ENABLE ROW LEVEL SECURITY;
```

**Why Row Level Security?** Supabase publishes tables in the `public` schema through a web API that anyone with your project's public key can call. With RLS on and no policies, that API returns nothing. Your pipeline connects as the database owner, which RLS doesn't restrict, so the pipeline still works.

### A3. Build the connection URL

1. Click **Connect** at the top of the project.
2. Choose the **Session pooler** connection string, in **URI** format. The pooler works on networks without IPv6, which includes Prefect's containers.
3. Replace `[YOUR-PASSWORD]` with your password. If the password contains special characters, percent-encode them: `@` → `%40`, `#` → `%23`, `/` → `%2F`.
4. Add `?sslmode=require` to the end, so the connection is always encrypted.

It looks like `postgresql://postgres.abcdefgh:p%40ssword@aws-0-eu-west-1.pooler.supabase.com:5432/postgres?sslmode=require`. **Treat it like a password.** Anyone with it can read and delete your data.

### A4. Teach `load.py` to speak PostgreSQL

```sh
uv add "psycopg[binary]"
```

In `src/my_pipeline/load.py`, add `import psycopg` under `import polars as pl`. Then replace `open_database` with this version:

```python
@contextmanager
def open_database(database: str) -> Iterator[tuple[sqlite3.Cursor | psycopg.Cursor, str]]:
    """Open the database, create the tables, and commit only if everything succeeds."""
    if database.startswith(("postgresql://", "postgres://")):
        with psycopg.connect(database, connect_timeout=10) as connection:  # Commits on success.
            connection.execute(TABLES)
            with connection.cursor() as cursor:
                yield cursor, "%s"
        return
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(TABLES)
        with connection:  # Commits on success, rolls back on any error.
            yield connection.cursor(), "?"
```

That's the whole change. A URL that starts with `postgresql://` goes to PostgreSQL, with `%s` placeholders. Anything else is a SQLite file, as before. Your SQL, upserts and transaction logic stay exactly the same. That's the payoff for using portable SQL in Lab 7.

### A5. Test against Supabase from your laptop

Put the URL in `.env`:

```sh
DATABASE_URL=postgresql://postgres.abcdefgh:...@...pooler.supabase.com:5432/postgres?sslmode=require
```

Run the pipeline (start the local Prefect server first, as in Lab 8):

```sh
uv run my-pipeline
```

In Supabase, open **Table Editor** and check that `weather_daily` and `weather_features` have rows. Run the pipeline again and check that the counts don't change. Idempotency works in PostgreSQL too.

Then **comment out** the line in `.env` (put `#` in front of it), so local work goes back to SQLite. Production gets the URL another way, in Part B.

---

## Part B · Keep the credential in Prefect

### B1. Create a Secret block

Switch your terminal to the cloud (`uv run prefect profile use cloud`). In [Prefect Cloud](https://app.prefect.cloud):

1. Open **Blocks** and click **+**.
2. Choose **Secret**.
3. Name it `my-pipeline-database`, and paste the full Supabase URL from A3 as the value.
4. Click **Create**.

### B2. Let the flow read it

In `src/my_pipeline/flow.py`, add this import:

```python
from prefect.blocks.system import Secret
```

Add a `database_secret` parameter to the flow, and use it to choose the database:

```python
@flow(name="weather-pipeline", log_prints=True)
def weather_pipeline(
    location: str = "accra",
    start: date | None = None,
    end: date | None = None,
    write_bronze: bool = True,
    database_secret: str | None = None,
) -> None:
    """Load daily weather for one location.

    With no dates, the flow catches up: it starts a few days before the newest stored
    date (the watermark) and stops at yesterday. Give start and end to backfill a range.
    Set database_secret to the name of a Prefect Secret block that holds a PostgreSQL URL.
    """
    # In Prefect Cloud the URL comes from a Secret block. Locally it comes from config (.env).
    database = Secret.load(database_secret).get() if database_secret else config.DATABASE
    # ... the rest of the flow is unchanged
```

The secret is only loaded **at run time, inside Prefect**. It never appears in your code, your Git history, your `prefect.yaml` or your GitHub settings. To change the password later, update the block. You don't change any code.

Run `uv run pytest` to check nothing broke.

---

## Part C · Describe the production deployment

### C1. Find your exact versions

The cloud container installs packages with `pip`, not from your lock file. So you tell it exactly which versions to install, and they must match `uv.lock`:

```sh
uv tree --depth 1 --no-dev
```

```text
my-pipeline v0.1.0
├── httpx v0.28.1
├── polars v2.1.0
├── prefect v3.9.3
├── psycopg[binary] v3.3.9
└── python-dotenv v1.2.4
```

### C2. Update `prefect.yaml`

Replace your `prefect.yaml` with this version. Use your own repository URL, and **your** versions from C1:

```yaml
# How Prefect Cloud gets and runs this project's code.
name: my-pipeline

# Before every run, Prefect clones the exact commit that CI tested and moves into src/.
pull:
  - prefect.deployments.steps.git_clone:
      id: clone
      repository: https://github.com/YOUR-USERNAME/my-pipeline.git
      commit_sha: "{{ $PREFECT_CODE_REVISION }}"
  - prefect.deployments.steps.set_working_directory:
      directory: "{{ clone.directory }}/src"

deployments:
  - name: hello
    entrypoint: my_pipeline/hello.py:hello_pipeline
    parameters:
      name: Prefect Cloud
    work_pool:
      name: my-managed-pool

  - name: daily-weather
    version: "{{ $PREFECT_CODE_REVISION }}"
    description: Load yesterday's weather for Accra into Supabase.
    entrypoint: my_pipeline/flow.py:weather_pipeline
    parameters:
      location: accra
      write_bronze: false # The cloud container's disk disappears after each run.
      database_secret: my-pipeline-database
    work_pool:
      name: my-managed-pool
      job_variables:
        image: prefecthq/prefect:3-python3.12
        # Installed before each run. Keep these equal to the versions in uv.lock.
        pip_packages:
          - prefect==3.9.3
          - polars==2.1.0
          - httpx==0.28.1
          - python-dotenv==1.2.4
          - "psycopg[binary]==3.3.9"
    concurrency_limit: 1
    schedules:
      - cron: "30 6 * * *"
        timezone: UTC
```

What changed since Lab 4:

- **`commit_sha` instead of `branch: main`.** `{{ $PREFECT_CODE_REVISION }}` is filled in from an environment variable when the deployment is registered. CI sets it to the commit it just tested, so production runs *exactly* the tested code, even if someone pushes again a minute later.
- **`version`** records that commit, so the Prefect UI shows which code a run used.
- **`image`** uses Python 3.12, the same as your `.python-version`.
- **`write_bronze: false`.** A Managed container's disk is deleted after each run, so a Bronze file there would vanish. (To keep Bronze in production, you'd write it to object storage such as Supabase Storage or S3. That's a good extension.)
- **`concurrency_limit: 1`** stops two runs from loading at the same time.

> [!NOTE]
> From now on, deployments are registered **by CI**, not from your laptop. To register one by hand anyway, set the variable first: `PREFECT_CODE_REVISION=$(git rev-parse HEAD) uv run prefect deploy …` from `src/`, after pushing that commit.

---

## Part D · Let GitHub Actions test and deploy

### D1. Write the workflow

Create `.github/workflows/ci.yml`:

```yaml
name: Check and deploy

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

jobs:
  checks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false
      - uses: astral-sh/setup-uv@07a047e608db84b3d8c06cd9a7c7dcc9c355c18e # v10.3.2
        with:
          version: "0.11.17"
      - run: uv sync --locked
      - run: uv run --locked ruff check .
      - run: uv run --locked ruff format --check .
      - run: uv run --locked pytest
      - name: Audit dependencies for known vulnerabilities
        env:
          RUST_LOG: error
        run: uv audit --locked --preview-features audit

  deploy:
    needs: checks
    if: github.ref == 'refs/heads/main' && github.event_name != 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false
      - uses: astral-sh/setup-uv@07a047e608db84b3d8c06cd9a7c7dcc9c355c18e # v10.3.2
        with:
          version: "0.11.17"
      - run: uv sync --locked --no-dev
      - name: Register this exact commit with Prefect Cloud
        working-directory: src
        env:
          PREFECT_API_URL: ${{ vars.PREFECT_API_URL }}
          PREFECT_API_KEY: ${{ secrets.PREFECT_API_KEY }}
          PREFECT_CODE_REVISION: ${{ github.sha }}
        run: uv run --locked --no-dev prefect deploy --prefect-file ../prefect.yaml --all --no-prompt
```

How to read it:

- **When it runs** (`on`): for every pull request into `main`, every push to `main`, and when you click **Run workflow**.
- **`checks`** installs exactly what `uv.lock` says, then runs your linter, formatter check, tests and a vulnerability audit. If any step fails, the job fails.
- **`deploy`** runs only after `checks` passes, and only on `main`, never for a pull request. It registers the commit it just tested.
- **Locked down:** `permissions: contents: read` gives the workflow read-only access. Actions are pinned to an exact commit (the long hex string), so a hijacked tag can't change what runs. `persist-credentials: false` keeps the Git token off the disk.

### D2. Give GitHub access to Prefect

1. In Prefect Cloud, click your profile picture → **API keys** → **Create API key**. Name it `github-actions` and set an expiry date. Copy the key, which is shown once.
2. Find your workspace URL. With the cloud profile active, run `uv run prefect config view` and copy the `PREFECT_API_URL` value.
3. Save both in GitHub. The URL is a **variable**, because it isn't secret. The key is a **secret**, which GitHub encrypts and hides from logs.

```sh
gh variable set PREFECT_API_URL --body "https://api.prefect.cloud/api/accounts/…/workspaces/…"
gh secret set PREFECT_API_KEY
```

`gh secret set` asks you to paste the value, so it never appears in your shell history.

**UI route:** in your GitHub repository, open **Settings → Secrets and variables → Actions**. Add `PREFECT_API_URL` on the **Variables** tab and `PREFECT_API_KEY` on the **Secrets** tab.

---

## Part E · Protect `main` and release through a pull request

### E1. Work on `dev` from now on

`main` is production. Do all your work on `dev`, and change `main` only through a pull request:

```sh
git switch -c dev
git add .
git commit -m "feat: run in production on Supabase"
git push -u origin dev
```

### E2. Open the first pull request

```sh
gh pr create --base main --head dev --title "Release: production pipeline" --body "First production release."
```

Open it on GitHub (`gh pr view --web`) and watch the **checks** job run. It must be green before you go on. If it's red, click **Details**, read the failing step, fix it on `dev`, and push. The pull request updates itself.

### E3. Turn on branch protection

Now that the `checks` job has run once, GitHub knows its name and you can require it. In your repository, open **Settings → Rules → Rulesets → New ruleset → New branch ruleset**:

1. **Ruleset name:** `protect-main`. **Enforcement status:** Active.
2. **Target branches:** click **Add target → Include default branch**.
3. Tick **Restrict deletions** and **Block force pushes**.
4. Tick **Require a pull request before merging**. Working alone? Set **Required approvals** to `0`, because GitHub doesn't let you approve your own pull request.
5. Tick **Require status checks to pass**, click **Add checks**, and choose `checks`.
6. Click **Create**.

Now nobody, including you, can push straight to `main` or merge a pull request with failing checks. The [security guide §4](../SECURITY.md#4-make-the-security-check-block-the-merge) has the same steps in more detail, and the equivalent `gh api` command.

### E4. Merge and watch it deploy

1. Merge the pull request (**Merge pull request** on GitHub, or `gh pr merge --merge`).
2. Open the **Actions** tab. The run on `main` runs `checks`, then `deploy`.
3. In Prefect Cloud, open **Deployments → daily-weather**. Its **version** is the commit you just merged. Compare it with `git rev-parse origin/main`.
4. Click **Run → Quick run** and open the run. The logs should show `Loading accra from …` and `Saved … rows`.
5. In Supabase **Table Editor**, the rows are there. Nothing ran on your laptop.

Then bring `dev` up to date, so your next change starts from what's in production:

```sh
git switch dev
git pull origin main
git push
```

### Your turn

Do all of the above for **your** pipeline: your tables in Supabase, your Secret block, your deployment with your schedule and pinned versions, your workflow, your ruleset. Then make one small, real change (a new location, a new Gold feature, a better log message) and release it the proper way: branch, pull request, green checks, merge, deploy.

## Checkpoint

- [ ] Your pipeline loads into Supabase from Prefect Cloud on a schedule, with nothing running on your laptop
- [ ] The database URL appears only in your `.env` (commented out) and the Prefect Secret block. Check with `git grep -n "pooler.supabase"`, which should print nothing
- [ ] A push straight to `main` is rejected, and a pull request can't merge until `checks` passes
- [ ] The deployment's version in Prefect Cloud matches the latest commit on `main`

## How the workshop repo does it

- The same pieces, at full size: [`ci.yml`](../../.github/workflows/ci.yml), [`prefect.yaml`](../../prefect.yaml), the Secret block (see `database_target` in [`_shared.py`](../../src/market_data/flows/_shared.py)), and [`sql/schema.sql`](../../sql/schema.sql) for Supabase.
- [The deployment guide](../DEPLOYMENT.md) walks through every step with a terminal route and a UI route. Use it when a step here is unclear.
- It adds a test that fails if the `pip_packages` pins drift from `uv.lock`. See [`tests/test_deployment.py`](../../tests/test_deployment.py). Copy that idea into your project.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `Unable to find block document named my-pipeline-database` | The Secret block is missing, or is in another workspace. Its name must equal `database_secret` in `prefect.yaml` exactly |
| `password authentication failed` | Wrong password in the URL, or a special character that isn't percent-encoded |
| `connection to server … failed: Network is unreachable` or a timeout | You used the *direct* connection string. Use the **Session pooler** one |
| `server does not support SSL` or `SSL required` | Add `?sslmode=require` to the URL (or `&sslmode=require` if it already has a `?`) |
| The cloud run fails with `ModuleNotFoundError: No module named 'psycopg'` | A package is missing from `pip_packages`. Every import in your code must be listed there |
| The `deploy` job fails with `401` or `403` | The API key is wrong or expired. Create a new one and run `gh secret set PREFECT_API_KEY` again |
| The `deploy` job never runs | It runs only on pushes to `main`, after `checks` passes. A pull request alone never deploys |
| `checks` isn't offered under **Add checks** | The workflow must have run at least once. Open a pull request first, then add the rule |
| `git push` to `main` is rejected | That's the ruleset doing its job. Push to `dev` and open a pull request |
| The deployment's version shows `{{ $PREFECT_CODE_REVISION }}` | Someone deployed without setting the variable. Deploy through CI, or set it as described in the note under C2 |

More fixes: [lab troubleshooting](troubleshooting.md#lab-9--production).

## Go further

- [GitHub: about rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets)
- [GitHub Actions: secure use](https://docs.github.com/en/actions/reference/security/secure-use)
- [Supabase: connecting to Postgres](https://supabase.com/docs/guides/database/connecting-to-postgres)
- [Prefect: store secrets](https://www.prefect.io/docs/v3/how-to-guides/configuration/store-secrets)
