# B101 Nest Session-Fix Build Notes

This branch applies the B101 Google Nest session-stability patch to `AlexxIT/go2rtc`.

Base commit:

```text
dc1685e9cf7a8c349181f20a1b4a44825ed394c5
```

Primary changed file:

```text
pkg/nest/api.go
```

## What This Branch Changes

- Stores Nest OAuth credentials on each `API` instance.
- Returns per-stream API clones from the shared token cache.
- Serializes Google SDM command calls with a global command lock.
- Adds deterministic per-device jitter before Nest stream extension.
- Converts Nest stream extension from one-shot timer behavior to a repeat loop.
- Retries controlled transient statuses in WebRTC generation and stream extension paths.
- Refreshes access token on `401`.
- Backs off for `409` and `429` without forcing token refresh.
- Closes HTTP response bodies in Nest command paths.
- Protects extension timer state with a mutex.

## Build From GitHub

```bash
set -euo pipefail

WORK="/root/go2rtc-fork-work"
SRC="$WORK/src/go2rtc-b101"
BUILDS="$WORK/builds"
OUT="$BUILDS/go2rtc-b101-nest-sessionfix-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$WORK/src" "$BUILDS"

if [ ! -d "$SRC/.git" ]; then
  git clone https://github.com/bober10113/go2rtc.git "$SRC"
fi

cd "$SRC"
git fetch origin
git checkout codex/b101-nest-sessionfix
git reset --hard origin/codex/b101-nest-sessionfix

gofmt -w pkg/nest/api.go
CGO_ENABLED=0 go build -o "$OUT" ./...
chmod +x "$OUT"

ls -lh "$OUT"
sha256sum "$OUT"
timeout 5 "$OUT" -version 2>&1 || true
go version -m "$OUT" | egrep 'path|mod|vcs.revision|vcs.modified|GOOS|GOARCH|CGO_ENABLED' || true
```

This creates a test binary only. It does not install or replace `/config/go2rtc`.

## Before Any Install

Confirm the currently running Frigate/go2rtc state first:

```bash
docker ps --filter name=frigate --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'

docker exec frigate sh -lc '
echo "go2rtc processes:"
pgrep -a go2rtc || true
echo
COUNT="$(pgrep go2rtc 2>/dev/null | wc -l | tr -d " ")"
echo "go2rtc process count: $COUNT"
timeout 5 /config/go2rtc -version 2>&1 || true
'
```

Expected:

```text
go2rtc process count: 1
```

Do not run:

```bash
/config/go2rtc version
```

Use:

```bash
timeout 5 /config/go2rtc -version 2>&1 || true
```

## Install Guardrail

The currently working Frigate binary at `/config/go2rtc` must not be replaced unless explicitly approved.

If a future test binary is approved for install, back up the current binary immediately before replacing it.

## Rollback Pattern

Rollback to the backup made immediately before a future install:

```bash
set -euo pipefail

COMPOSE_DIR="/root/frigate-docker"
BACKUP="$(ls -t /config/go2rtc.backup.before-* | head -1)"

test -x "$BACKUP"
echo "Using backup: $BACKUP"

install -m 0755 "$BACKUP" /config/go2rtc
timeout 5 /config/go2rtc -version 2>&1 || true

cd "$COMPOSE_DIR"
docker compose up -d --force-recreate frigate

sleep 120

docker exec frigate sh -lc '
pgrep -a go2rtc || true
COUNT="$(pgrep go2rtc 2>/dev/null | wc -l | tr -d " ")"
echo "go2rtc process count: $COUNT"
timeout 5 /config/go2rtc -version 2>&1 || true
'
```
