# B101 Nest Session-Fix Build Notes

This branch applies the B101 Google Nest session-stability patch to `AlexxIT/go2rtc`.

Base commit:

```text
dc1685e9cf7a8c349181f20a1b4a44825ed394c5
```

This document intentionally avoids private camera names, device names, tokens, URLs, and full device IDs.

## Current Safe Candidate

Current candidate: **v60 derived-publish failure reset test build**.

v60 is the current Frigate test binary installed as `/config/go2rtc` in the B101 Frigate CT at the time this note was updated.

Expected runtime version shape:

```text
go2rtc version 1.9.14+dev.<commit>.dirty (<commit>.dirty) linux/amd64
```

The `.dirty` suffix is expected for the first v60 test binary because it was built from the v59 branch tip plus the v60 local patch before this documentation commit archived it.

v60 keeps the v52-v59 Nest media-readiness and recovery work, then adds one focused change:

- If a local Nest-derived `exec:` RTSP publisher fails before it has produced usable media, go2rtc force-resets the matching raw `nest:` upstream even if that upstream still looks present.
- The reset is limited to early-publish/media-timeout style failures, not every generic `exec` failure.
- The intent is to avoid long downtime where Frigate keeps retrying a derived RTSP stream that exists in name but is not delivering usable media.

This document intentionally avoids private camera names, private stream names, full device IDs, credential URLs, and tokens.

## Recent Version Map

The version labels below are test-build labels for the B101 branch. They are not upstream go2rtc release versions.

- `b101-v52-baseline`: gate Nest derived media readiness before exposing a stream as usable.
- `b101-v53-derived-settle`: reset the raw Nest stream when derived settle fails.
- `b101-v54-h264-handoff`: reset H264 readiness after derived handoff.
- `b101-v55-waiter-handoff`: let Nest derived waiters use the handoff gate.
- `b101-v56-active-recovery-hold`: hold Nest derived producers during active recovery.
- `b101-v57-h264-handoff-gate`: require H264 handoff packets before the consumer is considered ready.
- `b101-v58-handoff-ready`: tighten Nest H264 handoff readiness before consumer use.
- `b101-v59-derived-backoff-clear`: clear stale Nest derived backoff after media recovery.
- `b101-v60-derived-publish-reset`: reset the raw Nest upstream after early derived publish/media failures.

## Canceled v10 Result

The v10 recovery-gate/watchdog build was installed on 2026-05-31 and then rolled back.

Sanitized result:

- Installed v10 binary: `1.9.14+dev.4e2b274`.
- Frigate container stayed healthy.
- Multiple Nest-derived RTSP/ffmpeg paths entered repeated local recovery and local RTSP `404 Not Found` loops after restart.
- The final short check before rollback did not show Google SDM `401`, `429`, or `extend failed` as the main signal.
- The issue was local media recovery behavior, not normal Nest extension timing.
- Rollback to `1.9.14+dev.dc6aa3c` returned the system to a clean immediate state.

Conclusion:

Do not deploy the v10 release binary again. v10 was too disruptive during restart recovery.

## Current Test Focus

The B101 logs showed two broad classes of issues:

- Nest API/session extension mostly worked, including recovery from short `401` token-refresh events.
- A later media-path burst affected derived Nest restreams with ffmpeg restart loops, invalid input, local RTSP `404`, and RTSP demux timeout behavior.

For v11, the priority is stability over new behavior. The branch keeps the safer pre-v10 work and removes the disruptive v10 recovery gate/watchdog changes.

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
- Holds local Nest-derived `exec:` RTSP starts for a short recovery window after an upstream reset, reducing rapid invalid-input and dimensions-not-set retries while the raw stream is not ready yet.
- Gives local Nest-derived `exec:` RTSP starts a longer startup window so raw stream regeneration has time to publish before the derived restream is declared failed again.
- Serializes and throttles `GenerateWebRtcStream` per device, reducing repeated session replacement when Frigate retries a recovering derived stream.
- Sends immediate burst RTCP picture-loss indications for Nest WebRTC video, then continues periodic keyframe requests, so reconnects are more likely to deliver SPS/PPS/keyframes before derived ffmpeg copy streams publish.
- Marks matching inactive Nest producers as reset/recovering when a derived stream fails after the raw producer has already been stopped.
- Treats `400` and `404` responses from Nest stream extension as terminal stale-session signals, stops that extension loop, and lets a fresh stream session be generated instead of retrying the dead session forever.
- Allows only one active Nest extension owner per device, so older extension loops are superseded when a replacement session is generated.
- Adds account-level `429 Too Many Requests` cooldown before more Google SDM commands are attempted.
- Redacts sensitive Nest source URLs and private local stream names from reset/timeout logs.
- Keeps repeated-failure recovery state during a short derived-stream publish probe, so a momentary local RTSP publish does not prematurely erase the outage history.

## Removed From v11

The following v10 behavior is intentionally not part of v11:

- active empty Nest producer watchdog
- extended raw-media recovery gate that can keep local RTSP returning `404`
- startup behavior that causes Frigate to repeatedly relaunch ffmpeg while go2rtc is intentionally withholding the derived stream

## Pre-v10 Log Observation - 2026-05-31

The pre-v10/v9 runtime logs showed the current issue is still in the local media recovery path, not in normal Google SDM extension handling.

Sanitized findings:

- Running binary during this sample: `1.9.14+dev.dc6aa3c`.
- Frigate/go2rtc restarted around 10:53 local time.
- First local media-path failure appeared around 11:09 local time.
- Google SDM command handling stayed clean in this sample:
  - no `400 Bad Request`
  - no `401 Unauthorized`
  - no `429 Too Many Requests`
  - no `extend failed`
- The visible failure pattern was local:
  - derived RTSP/ffmpeg read timeout
  - upstream `nest:` producer reset
  - Frigate retrying the derived RTSP stream while go2rtc reported `local nest upstream still recovering`
  - temporary local RTSP `404 Not Found` during the recovery window
- A later live check showed another short recovery loop around 11:24 local time, followed by recovery.
- Recent live stream state showed enabled Nest raw and derived streams active again.

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

gofmt -w pkg/nest/api.go internal/exec/exec.go internal/streams/producer.go internal/webrtc/webrtc.go
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "$OUT" .
chmod +x "$OUT"

ls -lh "$OUT"
sha256sum "$OUT"
timeout 5 "$OUT" -version 2>&1 || true
go version -m "$OUT" | egrep 'path|mod|trimpath|vcs.revision|vcs.modified|GOOS|GOARCH|CGO_ENABLED' || true
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
- any `local nest upstream still recovering` lines should be short-lived and followed by successful stream recovery
- no burst of repeated `session generated` / `extend superseded` lines for the same device during one recovery event
- no repeated `dimensions not set` loop after a Nest WebRTC reconnect
- no repeated one-minute `GenerateWebRtcStream` loop paired with immediate `extend stopped` for the same device
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
