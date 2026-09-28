# v64: sustained failed derived-media recovery

Refined September 28, 2026 following pre-deployment log review.
User authorized deployment; deployment evidence is recorded separately.

## Problem and scope

The existing Nest warmup and settle failure paths veto raw resets whenever raw
packet counters change. Raw traffic can continue without usable derived video,
so repeated failures can keep recreating the derived chain indefinitely.

This change is limited to local Nest-derived failure tracking and recovery.
Ordinary RTSP/Tapo/Tuya handling, API renewal, codecs, and Frigate configuration
are unchanged. No new background camera polling or API reset loop is added.

## Refined policy

- Share failed-media history by raw input name using the existing recovery state.
- Count confirmed stale-media observations as well as failed warmup/settle gates.
  Stalls use the observed stagnant duration to start the failure clock.
- Permit active-raw escalation after 60 seconds of failed recovery and at least
  two observations, not three completed long probes. The check occurs on a failed
  gate, not on every RTSP client request.
- Cap the next warmup probe to the remaining escalation budget, with a 500ms
  minimum. Counter rebasing respects that budget. Existing retry/API holds are
  not lengthened or bypassed.
- Rate-limit this new active-raw escalation to one reservation per two minutes,
  shared across consumers. Derived recovery continues during the interval.
- Preserve the existing inactive-raw reset thresholds and producer-level
  duplicate protection. The new two-minute cooldown does NOT block that path.
- Clear shared failure history on verified derived recovery, but retain the last
  reset timestamp so a brief recovery cannot erase active-raw rate limiting.
- Expire abandoned history using the existing ten-minute failure window.
- Log raw_activity_escalation=true when traffic no longer vetoes a reset.

## Why the initial candidate was held

The first candidate counted three completed gates. Against the observed long-gap
sequence, escalation would not become eligible until nearly six minutes after
recording loss. Its cooldown also covered inactive-raw resets unnecessarily.
That candidate was never installed. The refined trace permits escalation at
about 74 seconds and preserves the old inactive-source path.

## Tests and limits

Tests cover raw traffic during failure, elapsed-time and repeat safeguards,
warmup/settle shared history, source isolation, concurrent reset reservation,
readiness clearing, history expiry, actual settle-path reset requests, probe
deadline capping, and both observed failure traces using synthetic camera names.

Windows focused tests and Linux race checks passed ten repetitions. The Linux
amd64 binary built successfully. Full internal/streams still has TestRecursion and
TestTempate failures (`streams: source not supported`); both reproduce on an
unchanged v63 source archive at ec152af. Do not report the full suite as green.

These tests verify policy and wiring, not a guaranteed restoration deadline.
Failed source creation, API backoff, camera availability, keyframe acquisition,
and later Frigate/recording failures can still delay footage. A short burst
passing the existing ready gate clears failure history; false-positive readiness
remains a separate risk. No claim is made that this fixes every multi-minute gap.

## Deployment and evaluation

Back up the installed binary, configuration, Compose files and checksums.
Deploy only the binary with one Frigate restart; preserve image, NAS/GPU mappings
and camera settings. Publish sanitized source and the matching binary/checksum.
Verify installed hash/version, one go2rtc process, healthy Frigate, fresh finalized
recordings for every enabled camera, and startup errors.

Evaluate longer-run results using finalized recording gaps, not raw counters or
Generate success alone. Correlate raw_activity_escalation, raw_changed, verified
media and actual resumed recordings. Keep private logs, camera names and config
out of public source. Ask the user before reverting production.

## Deployment evidence - September 28, 2026

- Source and release tag b101-v64: a19816c7f8fdce25a3d20043cd34fb052389ca18.
- Published binary SHA-256 matches the installed file and running process:
  193e2bb4f6c61bb90c9fc33c527943ae97aad72d8e1d66820beabe9ad75f7625.
- Frigate restarted at 00:48:28 EDT. Version remains 0.18.0; persistent YAML
  and Compose files matched their pre-deployment checksums.
- Startup checks at approximately 49 and 107 seconds showed healthy Frigate,
  exactly one matching go2rtc process and fresh recordings from all eight cameras.
- The three Nest sources began recording at approximately 20-22 seconds;
  sampled finalized files contain 1600x1200 H264 video and AAC audio.
- No no-frame/read-frame/FFmpeg-crash/invalid-data/media-stall messages were found
  in those startup checks, and no completed inter-segment gap >=10 seconds was
  found after the first post-startup recordings.
- No active-raw escalation has been exercised in these startup checks.
  This is startup verification, not proof of sustained recovery or stability.
