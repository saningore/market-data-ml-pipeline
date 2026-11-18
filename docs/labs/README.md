# Lab sessions: build your own data pipeline

These labs take you from an empty folder to **your own** data pipeline, running in production on a schedule. You don't copy the workshop repository. You pick a data source you care about and build a pipeline for it, one concept at a time.

Each lab works the same way:

1. **Why it matters.** The idea in a few sentences.
2. **Worked example.** A small, tested example using daily weather from the free [Open-Meteo](https://open-meteo.com/) API. It is deliberately a *different* source from the workshop's gold prices, so you see each idea twice.
3. **Your turn.** Apply the same idea to the source you chose in Lab 1. This is the part that teaches you.
4. **Checkpoint.** What you should see before you move on.
5. **How the workshop repo does it.** Links into this repository, where the same idea is done at a larger scale.
6. **Stuck?** The most common problems for that lab. The [lab troubleshooting page](troubleshooting.md) has more.

> [!TIP]
> **Using an AI assistant?** [AI prompts](ai-prompts.md) has copy-and-fill prompts for every lab: finding a source, drafting your validation rules and tests, writing your Supabase SQL, debugging an error, and practising how to explain your pipeline. Use them to go faster on the mechanical parts, not to skip the thinking.

> [!TIP]
> No source in mind yet? Follow the weather example as your own pipeline. You can switch to another source later, and the steps stay the same.

## The labs

| Lab | You will | Time | Concepts |
| --- | --- | --- | --- |
| [0 · Set up your machine](lab-00-setup.md) | Install the tools and check they work | 30 min | Terminal, Git, uv, editor |
| [1 · Choose your data and plan](lab-01-plan.md) | Pick a source and write a one-page pipeline plan | 45 min | Source, record key, data contract, cadence |
| [2 · Create the project](lab-02-project.md) | Set up a clean Python project on GitHub | 45 min | Project structure, dependencies, `.gitignore`, secrets |
| [3 · Hello-world pipeline](lab-03-hello-pipeline.md) | Run a three-step pipeline with Prefect and watch it in the UI | 45 min | Flows, tasks, runs, logs |
| [4 · Deploy hello world](lab-04-deploy-hello.md) | Run the same pipeline in Prefect Cloud | 45 min | Deployments, work pools, `prefect.yaml` |
| [5 · Extract into Bronze](lab-05-extract-bronze.md) | Read your source and keep a raw copy | 60 min | Extract, Bronze, metadata, temporary vs permanent errors |
| [6 · Validate into Silver](lab-06-validate-silver.md) | Turn raw data into a typed, trusted table | 60 min | Data contracts, fail fast, tests |
| [7 · Gold and an idempotent load](lab-07-gold-and-load.md) | Compute features and save both tables safely | 75 min | Gold, primary keys, upserts, transactions |
| [8 · Orchestrate it](lab-08-orchestrate.md) | Add retries, catch-up, backfill and a schedule | 75 min | Watermarks, overlap, backfill, retries |
| [9 · Go to production](lab-09-production.md) | Run on a schedule in the cloud, released by CI | 2 h | PostgreSQL, secrets, CI/CD, protected `main` |
| [10 · Harden and hand over](lab-10-harden.md) | Handle schema drift, audit dependencies, write a runbook | 75 min | Schema drift, dependency audit, alerts, runbook |

**Total:** about 11 hours. Most students spread it over two or three weeks.

## Before you start

- You can open a terminal and run a command. Lab 0 shows you how.
- You know a little Python: functions, lists, dictionaries and `import`.
- Watch the [theory slides](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s) once. The labs assume you have seen the words *Bronze*, *Silver* and *Gold*.

## How you will know you're done

At the end of Lab 10 you have:

- [ ] A public GitHub repository with your pipeline, tests and a clear README
- [ ] A Prefect Cloud deployment that runs on a schedule and loads your source into a cloud database
- [ ] A `main` branch that only changes through a reviewed pull request with passing checks
- [ ] A short runbook: how to rerun, backfill, pause and debug your pipeline

That is also most of the [assignment](../STUDENT_ASSIGNMENT.md#part-2--build-your-own-pipeline). Lab 10 maps each rubric line to the lab that covers it.

## Coming next: MLOps labs

The labs continue when the course moves to machine learning. They will build on the pipeline you finished here.

| Lab | Topic | Course session |
| --- | --- | --- |
| 11 | Explore your Gold data and define a prediction target | End-to-end ML lifecycle |
| 12 | Time-aware train and test splits, and a baseline model | End-to-end ML lifecycle |
| 13 | Track experiments and tune a model with MLflow and Optuna | Automations using MLflow |
| 14 | Retrain on a schedule, gate releases, and serve predictions | Automations using MLflow |
| 15 | Run the same work on a cluster | Distributed vs single-thread compute |

Until then, the [MLOps kickoff](../MLOPS.md) and the [roadmap](../ROADMAP.md) show where we are heading.

## Getting help

1. Read the **Stuck?** table at the end of the lab, then the [lab troubleshooting page](troubleshooting.md).
2. Read the error from the bottom up. The last lines usually name the problem.
3. Ask an AI assistant with the [Debug an error](ai-prompts.md#debug-an-error) prompt. Remove secrets first.
4. Ask in the class channel. Paste the command you ran, the last 20 lines of output, and what you expected. **Never paste passwords, API keys or database URLs.**
