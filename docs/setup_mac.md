# Mac setup for developers

Do these once per Mac. Takes about 30 minutes. Ask Navtej if anything fails.

## 1. Tools

```bash
# Homebrew (skip if `brew --version` works)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Git, uv, Java 17, Databricks CLI
brew install git uv openjdk@17
brew install databricks/tap/databricks
```

If the Databricks CLI says "Command Line Tools are too outdated": System Settings -> General -> Software Update, or
`sudo rm -rf /Library/Developer/CommandLineTools && sudo xcode-select --install`, then retry.

## 2. Make Java 17 the default

```bash
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.zshrc
source ~/.zshrc
java -version      # must say 17
```

`SPARK_LOCAL_IP` stops Spark from failing when you switch Wi-Fi networks.

## 3. Get the project

```bash
git clone <repo-url> ~/Projects/sales-insights
cd ~/Projects/sales-insights
git config --global init.defaultBranch main
uv sync
uv run pytest
```

All tests must pass. The first run downloads Delta Lake's Java files, so it takes a minute.

## 4. Editor

VS Code with these extensions: **Python** (Microsoft), **Databricks** (Databricks). When asked for a Python interpreter, pick the one in `.venv`.

## Do NOT

- Install `databricks-connect` into this project. It conflicts with PySpark. It gets a separate environment on 15 Oct.
- Upgrade packages without asking (`uv.lock` keeps everyone on identical versions).
- Commit anything under `staging/`, `landing/` or `lake/`.
