# B101 Nest Session-Fix Build Notes

This branch applies the B101 Google Nest session-stability patch to `AlexxIT/go2rtc`.

Base commit:

```text
dc1685e9cf7a8c349181f20a1b4a44825ed394c5
```

This document intentionally avoids private camera names, device names, tokens, URLs, and full device IDs.

## Current Test Focus

The B101 logs showed two broad classes of issues:

- Nest API/session extension mostly worked, including recovery from short `401` token-refresh events.
- A later media-path burst affected derived Nest restreams with ffmpeg restart loops, invalid input, and RTSP demux timeout behavior.

That means the current branch focuses on both sides:

1. Safer Nest API/session handling.
2. Conservative stale media-path recovery when a local derived `exec:` RTSP stream fails before publishing.
3. Less aggressive retry behavior while a Nest upstream stream is being replaced.

## What This Branch Changes

- Stores Nest OAuth credentials on each `API` instance.
- Returns per-stream API clones from the shared token cache.
- Serializes Google SDM command calls with a global command lock.
- Logs safe Nest lifecycle details without secrets, camera names, full device IDs, or credential URLs.
- Logs Google SDM command start/end, status, attempt, lock wait, duration, token refresh, session generation, session extension, and extension scheduling.
- Uses bounded Nest HTTP timeouts:
  - OAuth/token refresh: 30s
  - GetDevices: 30s
  - Generate/Extend stream commands: 45s
  - Stop stream command: 30s
- Adds deterministic per-device jitter before Nest stream extension.
- Converts Nest stream extension from one-shot timer behavior to a repeat loop.
- Retries controlled transient statuses in WebRTC generation and stream extension paths.
- Refreshes access tokens on `401`.
- Backs off for `409` and `429` without forcing token refresh.
- Adds per-camera command failure cooldown after repeated failures.
- Closes HTTP response bodies in Nest command paths.
- Protects extension timer state with a mutex.
- Resets/reconnects a stale upstream `nest:` producer when a local derived `exec:` RTSP stream fails before publishing.
- Debounces duplicate upstream Nest resets so repeated derived-stream failures do not keep aborting the same replacement session.
- Uses a short Nest-specific reconnect delay after upstream reset/replacement, instead of immediately retrying into a stream that is still being recreated.
- Treats `400` and `404` responses from Nest stream extension as terminal stale-session signals, stops that extension loop, and lets a fresh stream session be generated instead of retrying the dead session forever.
- Allows only one active Nest extension owner per device, so older extension loops are superseded when a replacement session is generated.
- Adds account-level `429 Too Many Requests` cooldown before more Google SDM commands are attempted.
- Redacts sensitive Nest source URLs and private local stream names from reset/timeout logs.

## Download Test Binary From Release

After the GitHub Actions workflow publishes the prerelease, download the test binary directly on the Frigate LXC/host:

```bash
set -euo pipefail

WORK="/root/go2rtc-fork-work"
BUILDS="$WORK/builds"
BIN="$BUILDS/go2rtc-b101-nest-sessionfix-linux-amd64"
SHA="$BIN.sha256"
META="$BIN.go-version-m.txt"
BASE_URL="https://github.com/bober10113/go2rtc/releases/download/b101-nest-sessionfix-test"

mkdir -p "$BUILDS"
cd "$BUILDS"

curl -fL -o "$(basename "$BIN")" "$BASE_URL/go2rtc-b101-nest-sessionfix-linux-amd64"
curl -fL -o "$(basename "$SHA")" "$BASE_URL/go2rtc-b101-nest-sessionfix-linux-amd64.sha256"
curl -fL -o "$(basename "$META")" "$BASE_URL/go2rtc-b101-nest-sessionfix-linux-amd64.go-version-m.txt"

chmod +x "$BIN"
sha256sum -c "$(basename "$SHA")"
timeout 5 "$BIN" -version 2>&1 || true
go version -m "$BIN" | egrep 'path|mod|vcs.revision|vcs.modified|GOOS|GOARCH|CGO_ENABLED' || true
```

This downloads and verifies a test binary only. It does not install or replace `/config/go2rtc`.

## Build From GitHub Source

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

gofmt -w pkg/nest/api.go internal/exec/exec.go internal/streams/producer.go
CGO_ENABLED=0 go build -o "$OUT" .
chmod +x "$OUT"

ls -lh "$OUT"
sha256sum "$OUT"
timeout 5 "$OUT" -version 2>&1 || true
go version -m "$OUT" | egrep 'path|mod|vcs.revision|vcs.modified|GOOS|GOARCH|CGO_ENABLED' || true
```

This creates a test binary only. It does not install or replace `/config/go2rtc`.

## GitHub Actions Test Build

This branch includes a manual/branch build workflow at:

```text
.github/workflows/build-b101-go2rtc.yml
```

If GitHub Actions is enabled for the fork, every push to `codex/b101-nest-sessionfix` and every manual run of the workflow builds a Linux amd64 artifact named:

```text
go2rtc-b101-nest-sessionfix-linux-amd64
```

The workflow also publishes the same files to the prerelease tag:

```text
b101-nest-sessionfix-test
```

That artifact/release asset is only a test binary. Downloading it does not install it into Frigate.

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

## Post-Test Validation

After a future approved install, collect at least 60 minutes of data and check:

```bash
bash /root/check-nest-only-reencode-health.sh 60
bash /root/audit-recording-coverage.sh 60
```

Healthy signs:

- exactly one go2rtc process
- no 429 storm
- no repeated 400/404 extension loop on the same stale Nest session
- no repeated exec timeout loop
- no repeated dimensions-not-set loop
- no repeated invalid-data loop
- no active camera recording gaps over 2 minutes
- enabled Nest cameras remain active
- disabled cameras remain disabled

Also inspect go2rtc logs for safe Nest lifecycle lines:

```bash
docker logs frigate 2>&1 | grep '\[nest\]' | tail -100
```

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
