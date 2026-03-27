#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
else
  echo "缺少环境变量文件: $ENV_FILE" >&2
  exit 1
fi

if [[ -x "$REPO_ROOT/.venv311/bin/python" ]]; then
  PYTHON_BIN="$REPO_ROOT/.venv311/bin/python"
elif [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif command -v python3.11 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3.11)"
else
  PYTHON_BIN="$(command -v python3)"
fi

cd "$REPO_ROOT"

exec "$PYTHON_BIN" "$REPO_ROOT/main.py" \
  --intraday-market-daemon \
  --intraday-region cn \
  "$@"
