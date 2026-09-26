#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
site_dir="${1:-$(cd "${script_dir}/.." && pwd)}"
public_dir="${site_dir}/public"

fail() {
  printf 'verify-site: %s\n' "$*" >&2
  exit 1
}

require_file() {
  [[ -f "$1" ]] || fail "missing file: $1"
}

require_text() {
  local file="$1"
  local text="$2"
  require_file "${file}"
  grep -Fq -- "${text}" "${file}" || fail "missing text '${text}' in ${file}"
}

require_absent_text() {
  local file="$1"
  local text="$2"
  require_file "${file}"
  if grep -Fq -- "${text}" "${file}"; then
    fail "unexpected text '${text}' in ${file}"
  fi
}

require_regex() {
  local file="$1"
  local regex="$2"
  require_file "${file}"
  grep -Eq -- "${regex}" "${file}" || fail "missing pattern '${regex}' in ${file}"
}

require_job_permissions() {
  local file="$1"
  local job="$2"
  shift 2
  require_file "${file}"
  awk -v job="${job}" -v expected="$*" '
    $0 == "  " job ":" { in_job = 1; next }
    in_job && /^  [[:alnum:]_-]+:$/ { exit }
    in_job && /^    permissions:$/ { in_permissions = 1; next }
    in_permissions && /^    [^ ]/ { in_permissions = 0 }
    in_permissions && /^      [[:alnum:]_-]+: / {
      line = $0
      sub(/^      /, "", line)
      split(line, parts, /: /)
      actual = actual (actual == "" ? "" : " ") parts[1] "=" parts[2]
    }
    END { exit actual == expected ? 0 : 1 }
  ' "${file}" || fail "job '${job}' permissions are not exactly: $*"
}

require_job_if() {
  local file="$1"
  local job="$2"
  local condition="$3"
  require_file "${file}"
  awk -v job="${job}" -v expected="    if: ${condition}" '
    $0 == "  " job ":" { in_job = 1; next }
    in_job && /^  [[:alnum:]_-]+:$/ { exit }
    in_job && $0 == expected { found = 1 }
    END { exit found ? 0 : 1 }
  ' "${file}" || fail "job '${job}' is missing ref/event gate: ${condition}"
}

require_step_text() {
  local file="$1"
  local job="$2"
  local step="$3"
  local expected="$4"
  require_file "${file}"
  awk -v job="${job}" -v step="${step}" -v expected="${expected}" '
    $0 == "  " job ":" { in_job = 1; next }
    in_job && /^  [[:alnum:]_-]+:$/ { exit }
    in_job && $0 == "      - name: " step { in_step = 1; next }
    in_step && /^      - name: / { exit }
    in_step && $0 == expected { found = 1 }
    END { exit found ? 0 : 1 }
  ' "${file}" || fail "step '${step}' in job '${job}' is missing: ${expected}"
}

require_glob() {
  compgen -G "$1" >/dev/null || fail "missing generated file matching: $1"
}

require_file "${site_dir}/hugo.yaml"
require_file "${site_dir}/go.mod"
require_file "${site_dir}/go.sum"
require_file "${site_dir}/content/_index.md"
require_file "${site_dir}/content/docs/_index.md"
require_text "${site_dir}/hugo.yaml" "min: 0.165.0"
require_file "${public_dir}/index.html"
require_file "${public_dir}/index.md"
require_file "${public_dir}/index.xml"
require_file "${public_dir}/llms.txt"
require_file "${public_dir}/sitemap.xml"
require_file "${public_dir}/robots.txt"
require_file "${public_dir}/404.html"
require_file "${public_dir}/docs/index.xml"
require_file "${public_dir}/_print/docs/index.html"
require_glob "${public_dir}/offline-search-index.*.json"
require_text "${public_dir}/index.html" "eventbus-contract"
require_text "${public_dir}/index.html" "<link rel=canonical href=https://mapseekai.github.io/eventbus-contract/>"
require_regex "${public_dir}/index.html" '<link[^>]+href=/eventbus-contract/scss/[^ >]+\.css[^>]+rel=stylesheet'
require_regex "${public_dir}/index.html" '<script[^>]+src=/eventbus-contract/js/[^ >]+\.js'
require_regex "${public_dir}/index.html" 'data-td-index-src=/eventbus-contract/offline-search-index\.[^ >]+\.json'
require_text "${public_dir}/index.html" "href=https://docs.rs/eventbus-contract/0.2.3/eventbus_contract/"
require_text "${public_dir}/llms.txt" "文档"
require_text "${public_dir}/robots.txt" "Allow: /"
require_text "${public_dir}/robots.txt" "Sitemap:"
require_file "${public_dir}/docs/start/index.html"
require_file "${public_dir}/docs/start/overview/index.html"
require_file "${public_dir}/docs/start/installation/index.html"
require_file "${public_dir}/docs/start/quickstart/index.html"
require_file "${public_dir}/docs/start/quickstart/index.md"
require_text "${public_dir}/docs/start/installation/index.md" "redis-watermill"
require_text "${public_dir}/docs/start/installation/index.md" "| 功能特性 | 默认启用 | 用途 |"
require_absent_text "${public_dir}/docs/start/installation/index.md" "| Feature | Default | Purpose |"
require_text "${public_dir}/docs/start/overview/index.md" "消费者组"
require_text "${public_dir}/docs/start/quickstart/index.md" "AckMode::Manual"
require_text "${public_dir}/docs/start/quickstart/index.md" "先订阅，再发布"
require_text "${public_dir}/docs/start/quickstart/index.md" 'br#"{"user_id":42}"#'
require_text "${public_dir}/llms.txt" "五分钟快速开始"
require_file "${public_dir}/docs/guides/memory/index.html"
require_file "${public_dir}/docs/guides/redis/index.html"
require_file "${public_dir}/docs/guides/watermill/index.html"
require_file "${public_dir}/docs/guides/competing-consumers/index.html"
require_text "${public_dir}/docs/guides/redis/index.md" "stream_bus_from_connection"
require_text "${public_dir}/docs/guides/redis/index.md" 'redis = { version = "1", features = ["tokio-comp", "streams"] }'
require_text "${public_dir}/docs/guides/redis/index.md" "eventbus_contract::redis::stream_bus_from_connection"
require_text "${public_dir}/docs/guides/redis/index.md" "Pending Entries List"
require_text "${public_dir}/docs/guides/redis/index.md" "重新认领"
require_text "${public_dir}/docs/guides/watermill/index.md" "redis-watermill"
require_text "${public_dir}/docs/guides/competing-consumers/index.md" "max_in_flight"
require_file "${public_dir}/docs/concepts/message-envelope/index.html"
require_file "${public_dir}/docs/concepts/contracts/index.html"
require_file "${public_dir}/docs/concepts/delivery/index.html"
require_file "${public_dir}/docs/concepts/guarantees-idempotency/index.html"
require_file "${public_dir}/docs/concepts/ordering-backpressure/index.html"
require_text "${public_dir}/docs/concepts/contracts/index.html" "data-td-diagram-source"
require_text "${public_dir}/docs/concepts/delivery/index.md" "AutoOnHandlerSuccess"
require_text "${public_dir}/docs/concepts/delivery/index.md" "| 模式 | 完成时机 | 适用场景 |"
require_absent_text "${public_dir}/docs/concepts/delivery/index.md" "| Mode | Finalization | Use |"
require_text "${public_dir}/docs/concepts/delivery/index.md" "待确认（pending）"
require_text "${public_dir}/docs/concepts/delivery/index.md" "重新认领（reclaim）"
require_text "${public_dir}/docs/concepts/delivery/index.md" "死信主题"
require_text "${public_dir}/docs/concepts/delivery/index.md" "重试预算"
require_text "${public_dir}/docs/concepts/delivery/index.md" "没有配置 dead_letter_topic 时，nack 会确认并丢弃原消息。"
require_text "${public_dir}/docs/concepts/guarantees-idempotency/index.md" "PublishConfirmation::Persisted"
require_text "${public_dir}/docs/concepts/ordering-backpressure/index.md" "max_pending_acks >= max_in_flight > 0"

require_file "${public_dir}/docs/reference/crates-features/index.html"
require_file "${public_dir}/docs/reference/configuration/index.html"
require_file "${public_dir}/docs/reference/errors/index.html"
require_file "${public_dir}/docs/reference/examples-commands/index.html"
require_file "${public_dir}/docs/contributing/workspace-architecture/index.html"
require_file "${public_dir}/docs/contributing/backend/index.html"
require_file "${public_dir}/docs/contributing/testing/index.html"
require_file "${public_dir}/docs/contributing/release/index.html"
require_text "${public_dir}/docs/reference/crates-features/index.md" "docs.rs"
require_text "${public_dir}/docs/reference/crates-features/index.md" "# 包与功能特性"
require_text "${public_dir}/docs/reference/crates-features/index.md" "## 门面包功能特性"
require_text "${public_dir}/docs/reference/crates-features/index.md" "| 包 | 作用 | 0.2.x 发布状态 |"
require_text "${public_dir}/docs/reference/crates-features/index.md" "| 功能特性 | 默认启用 | 含义 |"
require_absent_text "${public_dir}/docs/reference/crates-features/index.md" "# Crate 与 Feature"
require_absent_text "${public_dir}/docs/reference/crates-features/index.md" "## Facade features"
require_absent_text "${public_dir}/docs/reference/crates-features/index.md" "| Feature | 默认 | 含义 |"
require_text "${public_dir}/docs/reference/errors/index.md" "| 枚举变体 | 用途 |"
require_absent_text "${public_dir}/docs/reference/errors/index.md" "| Variant | 用途 |"
require_text "${public_dir}/docs/reference/configuration/index.md" "normalize_and_validate"
require_text "${public_dir}/docs/contributing/backend/index.md" "## 契约符合性清单"
require_absent_text "${public_dir}/docs/contributing/backend/index.md" "## Conformance checklist"
require_text "${public_dir}/docs/contributing/workspace-architecture/index.html" "data-td-diagram-source"
require_text "${public_dir}/docs/contributing/release/index.md" "MIGRATION-0.2.md"
require_text "${public_dir}/docs/contributing/release/index.md" "https://github.com/mapseekai/eventbus-contract/blob/main/CHANGELOG.md"
require_text "${public_dir}/docs/contributing/release/index.md" "https://github.com/mapseekai/eventbus-contract/blob/main/MIGRATION-0.2.md"
require_text "${public_dir}/docs/contributing/workspace-architecture/index.html" "href=https://github.com/mapseekai/eventbus-contract/edit/main/website/content/docs/contributing/workspace-architecture.md"
require_text "${public_dir}/docs/contributing/workspace-architecture/index.html" "href=https://github.com/mapseekai/eventbus-contract/commits/main/website/content/docs/contributing/workspace-architecture.md"
require_text "${public_dir}/docs/contributing/workspace-architecture/index.html" "third_party/mermaid/"

repo_root="$(cd "${site_dir}/.." && pwd)"
require_file "${repo_root}/.github/workflows/docs.yml"
require_text "${repo_root}/.github/workflows/docs.yml" "HUGO_VERSION: 0.165.0"
require_text "${repo_root}/.github/workflows/docs.yml" "actions/deploy-pages@v5"
if grep -Eq '^permissions:$' "${repo_root}/.github/workflows/docs.yml"; then
  fail "workflow-level permissions are forbidden"
fi
require_job_permissions "${repo_root}/.github/workflows/docs.yml" build "contents=read" "pages=read"
require_job_permissions "${repo_root}/.github/workflows/docs.yml" deploy "pages=write" "id-token=write"
deploy_gate="github.ref == 'refs/heads/main' && github.event_name != 'pull_request'"
require_job_if "${repo_root}/.github/workflows/docs.yml" deploy "${deploy_gate}"
require_step_text "${repo_root}/.github/workflows/docs.yml" build "Check out source" "          persist-credentials: false"
require_step_text "${repo_root}/.github/workflows/docs.yml" build "Configure GitHub Pages" "        if: ${deploy_gate}"
require_step_text "${repo_root}/.github/workflows/docs.yml" build "Upload Pages artifact" "        if: ${deploy_gate}"
require_text "${repo_root}/README.md" "https://mapseekai.github.io/eventbus-contract/"

printf 'verify-site: baseline contract passed\n'
