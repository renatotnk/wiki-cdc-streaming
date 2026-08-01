#!/usr/bin/env bash
# Shared helpers for scripts/cloud_*.sh -- meant to be sourced, not run
# directly (no shebang execution, no set -e here so it doesn't fight the
# caller's own error handling).

ensure_env_file() {
  local env_file="${1:-.env}"
  [ -f "$env_file" ] || cp .env.example "$env_file"
}

set_env_var() {
  local key="$1" value="$2" file="${3:-.env}"
  if grep -q "^${key}=" "$file" 2>/dev/null; then
    sed -i "s#^${key}=.*#${key}=${value}#" "$file"
  else
    echo "${key}=${value}" >>"$file"
  fi
}

comment_env_var() {
  local key="$1" file="${2:-.env}"
  sed -i "s#^${key}=#\#${key}=#" "$file"
}

uncomment_env_var() {
  local key="$1" file="${2:-.env}"
  sed -i "s#^\#${key}=#${key}=#" "$file"
}
