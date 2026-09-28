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

Windows focused tests pass ten repetitions. Linux race checks and build are
required before install. Full internal/streams still has TestRecursion and
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
