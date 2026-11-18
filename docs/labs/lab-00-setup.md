# Lab 0 · Set up your machine

**Time:** 30 minutes · **You will:** install the tools every later lab uses and check they work.

[All labs](README.md) · Next: [Lab 1 · Choose your data and plan](lab-01-plan.md)

## Why it matters

A pipeline is code that other people (and servers) must be able to run exactly as you do. The tools below make that possible. **Git** records every change. **uv** installs the same Python and the same package versions everywhere. **GitHub** stores your code and runs your checks. Install them once and the rest of the labs go smoothly.

## Steps

### 1. Open a terminal

| System | How |
| --- | --- |
| macOS | Press `Cmd + Space`, type **Terminal**, press Enter |
| Windows | Press the Windows key, type **PowerShell**, press Enter |
| Linux | Press `Ctrl + Alt + T` |

Every command in these labs goes into this window. Paste a command, press Enter, and read the output before you run the next one.

> [!NOTE]
> The labs show macOS and Linux commands. When Windows PowerShell needs something different, the lab says so.

### 2. Install Git

| System | Command or download |
| --- | --- |
| macOS | `xcode-select --install`, then click **Install** |
| Windows | Download and run [Git for Windows](https://git-scm.com/download/win). Keep the default options |
| Linux (Ubuntu) | `sudo apt install git` |

Close and reopen the terminal, then check:

```sh
git --version
```

Tell Git who you are. Use the email address of your GitHub account. The last line makes new repositories start on a branch called `main`.

```sh
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
git config --global init.defaultBranch main
```

### 3. Install uv and Python

[uv](https://docs.astral.sh/uv/) installs Python, creates a private environment for each project, and pins every package version in a lock file.

macOS and Linux:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Close and reopen the terminal, then install Python 3.12:

```sh
uv --version
uv python install 3.12
```

You don't need to install Python any other way. If you already have Python, that's fine. uv keeps its own copy and doesn't touch yours.

### 4. Install an editor

Install [Visual Studio Code](https://code.visualstudio.com/). Open it, go to **Extensions** (the four-squares icon), and install **Python** (by Microsoft). Any editor works, but the labs describe VS Code.

### 5. Create a GitHub account and the GitHub CLI

1. Sign up at [github.com](https://github.com/signup) if you don't have an account.
2. Install the GitHub CLI, `gh`. It lets you create repositories and pull requests from the terminal.

| System | Command |
| --- | --- |
| macOS | `brew install gh` (needs [Homebrew](https://brew.sh/)), or download it from [cli.github.com](https://cli.github.com/) |
| Windows | `winget install --id GitHub.cli` |
| Linux | Follow [the official instructions](https://github.com/cli/cli/blob/trunk/docs/install_linux.md) |

3. Sign in. Choose **GitHub.com**, **HTTPS**, and **Login with a web browser**, then follow the prompts.

```sh
gh auth login
gh auth status
```

`gh auth login` also lets plain `git push` use your GitHub account, so you won't be asked for a password later.

## Checkpoint

Run all five. Each should print a version or your account, not an error.

```sh
git --version
uv --version
uv run --python 3.12 python --version
gh auth status
code --version
```

If `code` isn't found on macOS, open VS Code, press `Cmd + Shift + P`, and run **Shell Command: Install 'code' command in PATH**.

> [!TIP]
> **Using AI?** Copy-and-fill prompts for this lab are in [AI prompts](ai-prompts.md#lab-0--setup). You still need to be able to explain every line you commit.

## Stuck?

| Symptom | Fix |
| --- | --- |
| `command not found: uv` (or `git`, `gh`) right after installing | Close the terminal completely and open a new one. Installers change your `PATH`, and only new terminals see it |
| Windows: `running scripts is disabled on this system` | Use the exact uv command above, which includes `-ExecutionPolicy ByPass`. Don't change the system-wide policy |
| `gh auth login` opens no browser | Copy the one-time code it prints, open the URL it shows, and paste the code there |
| Behind a school or company proxy, downloads fail | Ask your network admin for the proxy address, or use a different network for the installs |

More fixes: [lab troubleshooting](troubleshooting.md#lab-0--setup).

## Go further

- [The Missing Semester: shell tools](https://missing.csail.mit.edu/2020/course-shell/) explains the terminal properly.
- [Pro Git, chapters 1 and 2](https://git-scm.com/book/en/v2) is the best free introduction to Git.
