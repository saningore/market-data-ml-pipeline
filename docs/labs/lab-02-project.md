# Lab 2 · Create the project

**Time:** 45 minutes · **You will:** create a clean Python project, add the tools a pipeline needs, and publish it on GitHub.

[All labs](README.md) · Previous: [Lab 1](lab-01-plan.md) · Next: [Lab 3 · Hello-world pipeline](lab-03-hello-pipeline.md)

## Why it matters

A pipeline lives for months or years, and other people will run it. A good structure answers their questions before they ask:

- Where is the code? In `src/`.
- Where are the tests? In `tests/`.
- How do I install it? `uv sync`.
- Which versions? Pinned in `uv.lock`.
- Where do secrets go? In `.env`, which Git never sees.

## Steps

### 1. Choose a name

Pick a short name for your project, in lowercase with hyphens, such as `air-quality-pipeline` or `fx-rates-pipeline`. Python turns the hyphens into underscores for the package name: `air_quality_pipeline`.

> [!NOTE]
> The labs write **`my-pipeline`** for the project and **`my_pipeline`** for the package. Whenever you see those, type your own names instead.

### 2. Create the project

Go to the folder where you keep your code (for example `cd ~/code`), then run:

```sh
uv init --package --python 3.12 my-pipeline
cd my-pipeline
```

`--package` creates a **src layout**: your code lives in `src/my_pipeline/` and is installed into the environment like a real package. It catches import mistakes early, and it is what the deployment in Lab 4 expects.

You now have:

```text
my-pipeline/
├── .git/                  # uv already ran git init
├── .gitignore
├── .python-version        # 3.12: the Python version for this project
├── README.md
├── pyproject.toml         # project name, dependencies and tool settings
└── src/
    └── my_pipeline/
        └── __init__.py    # makes my_pipeline an importable package
```

Make sure the branch is called `main`. If you set `init.defaultBranch` in Lab 0 this changes nothing.

```sh
git branch -M main
```

### 3. Add the dependencies

```sh
uv add prefect polars httpx python-dotenv
uv add --dev ruff pytest
```

| Package | Job |
| --- | --- |
| `prefect` | Orchestration: runs your steps in order, retries them, schedules them and shows them in a UI |
| `polars` | Fast DataFrames for validating and transforming data |
| `httpx` | Calls web APIs. If your source is a file, you may not need it |
| `python-dotenv` | Reads settings from a local `.env` file |
| `ruff` (dev) | Finds mistakes and formats your code |
| `pytest` (dev) | Runs your tests |

Three things changed:

- **`pyproject.toml`** lists what you asked for, with minimum versions.
- **`uv.lock`** records the *exact* version of every package, including the packages they depend on. Commit it, so everyone installs the same thing.
- **`.venv/`** is the project's private environment. Never commit it. `uv sync` rebuilds it from the lock file.

### 4. Configure Ruff

Open `pyproject.toml` and add this at the end:

```toml
[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
```

These rules catch errors (`E`, `F`), sort imports (`I`), suggest modern Python (`UP`) and flag likely bugs (`B`).

### 5. Add folders and keep secrets out of Git

```sh
mkdir -p tests docs data
```

Open `.gitignore` and add these lines at the end:

```gitignore
# Local settings and secrets
.env

# Data produced by the pipeline
data/
*.sqlite3
```

Create `.env.example`. It lists the settings your project uses, **without real values**, so others know what to set:

```sh
# Copy this file to .env and fill it in. .env is ignored by Git.
# Local default: a SQLite file. In Lab 9 this becomes a PostgreSQL URL.
DATABASE_URL=
BRONZE_DIR=
```

Then make your own copy:

```sh
cp .env.example .env
```

In PowerShell: `Copy-Item .env.example .env`.

> [!WARNING]
> Prefect also reads `.env`. Don't put `PREFECT_API_URL` or `PREFECT_API_KEY` in it, or you'll silently connect to the wrong Prefect server later. Lab 3 uses Prefect profiles for that instead.

Move the plan you wrote in Lab 1 into `docs/plan.md`.

### 6. Write a first test

Create `tests/test_smoke.py`:

```python
def test_the_package_imports():
    import my_pipeline

    assert my_pipeline is not None
```

A *smoke test* proves the basics work. It looks silly, but it fails loudly if the package or environment is broken.

### 7. Run the checks

```sh
uv run my-pipeline
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

`uv run my-pipeline` calls the `main` function that `uv init` put in `__init__.py`. The `[project.scripts]` section of `pyproject.toml` connects the command name to that function. If `ruff format --check` lists files, run `uv run ruff format .` to fix them.

### 8. Commit and publish on GitHub

```sh
git add .
git status
git commit -m "chore: create the project"
gh repo create my-pipeline --public --source . --remote origin --push
```

**Read the `git status` output before you commit.** You should see `.env.example` but **not** `.env`, `.venv/` or `data/`. If `.env` appears, fix `.gitignore` first.

The repository must be **public** for the Prefect Cloud lab: Prefect clones it without a password. Private repositories work too, but need an access token (see [Lab 4](lab-04-deploy-hello.md#stuck)).

Open the repository on GitHub with `gh repo view --web`.

### 9. Open the project in VS Code

```sh
code .
```

Press `Cmd + Shift + P` (Windows: `Ctrl + Shift + P`), run **Python: Select Interpreter**, and choose the one in `.venv`. Now VS Code understands your imports.

### Your turn

> [!TIP]
> **Using AI?** Copy-and-fill prompts for this lab are in [AI prompts](ai-prompts.md#lab-2--create-the-project). You still need to be able to explain every line you commit.

Write two sentences at the top of `README.md`: what data your pipeline collects, and from where. Commit and push:

```sh
git add README.md
git commit -m "docs: describe the pipeline"
git push
```

## Checkpoint

- [ ] `uv run pytest` shows `1 passed`
- [ ] Your repository is on GitHub with `pyproject.toml`, `uv.lock`, `.env.example`, `src/`, `tests/` and `docs/plan.md`
- [ ] `git ls-files | grep -E '^\.env$'` prints nothing, so your `.env` is not in Git

## How the workshop repo does it

- Same layout: [`src/market_data/`](../../src/market_data/), [`tests/`](../../tests/), [`pyproject.toml`](../../pyproject.toml) and [`uv.lock`](../../uv.lock).
- [`.env.example`](../../.env.example) lists every setting with a comment.
- CI even fails if someone commits a `.env` file. See the "Check that local environment files are untracked" step in [`ci.yml`](../../.github/workflows/ci.yml).

## Stuck?

| Symptom | Fix |
| --- | --- |
| `uv init` says the project already exists | You're inside an existing project. `cd ..` and choose an empty folder |
| `uv run pytest` prints `no tests ran` | The test file must be named `test_*.py` and the function must start with `test_` |
| `ModuleNotFoundError: No module named 'my_pipeline'` | You renamed the folder but not the package, or the other way round. The folder under `src/` and the name in `[project.scripts]` must match |
| `gh repo create` says the name exists | You already have a repository with that name. Pick another name, or delete the old one on GitHub |
| You committed `.env` by mistake | Run `git rm --cached .env`, add `.env` to `.gitignore`, and commit. **Then change every password or key that was in it**, because Git history keeps the old file. See [Security §5](../SECURITY.md#5-if-a-secret-leaks) |

More fixes: [lab troubleshooting](troubleshooting.md#lab-2--project).

## Go further

- [uv: working on projects](https://docs.astral.sh/uv/guides/projects/)
- [Conventional Commits](https://www.conventionalcommits.org/) explains the `chore:` and `docs:` prefixes.
