# v63 Recovery Follow-Up Candidate

Status: candidate, not deployed. Base: v62, commit `222d37f`.
The live Frigate image remains 0.18.0-rc1. No compose, camera configuration,
FFmpeg settings, recording retention, audio conversion, or production binary
was changed during this audit.

## Evidence

The 2026-09-04 18:45:55 to 2026-09-05 13:50:56 EDT audit used finalized
SQLite recording intervals with duration >= 1 second. It merged overlapping
intervals before measuring uncovered time. This measures the recording index,
not playback integrity of every media file.

- One Nest camera had a 62m36s gap, beginning 00:11:55. Early generations
  returned HTTP 200 without usable media. From 00:16:05 to 01:11:54, 48
  GenerateWebRtcStream requests returned HTTP 400. A new session succeeded at
  01:13:55; recording resumed at 01:14:31. The old client discarded error
  bodies, so the underlying reason for those 400 responses is unknown.
- Another Nest camera had a 10m25s gap, beginning 11:38:48. Raw media was
  reported ready at 11:47:52. At that same time, local idle-producer stops and
  a 45-second publish-recovery block occurred. Recording resumed at 11:49:13.
- A new all-Nest interruption began around 14:03:47, outside that completed
  historical audit. Commands took the full 45-second HTTP timeout; subsequent
  commands waited over three minutes in the global command mutex. Fresh OAuth
  and read-only SDM device-list requests succeeded during the interruption.
  This does not prove whether stream-command timeouts came from Google, a
  network path, or an old pooled connection. Non-Nest recording continued.

## Changes

1. Keep the 45-second post-publish observation deadline separate from the
   retry cooldown. Publishing successfully no longer renews an expired
   publish-timeout penalty. Actual failed probes still back off.
2. Give probe callbacks unique IDs across deleted/recreated recovery state.
   New failures invalidate an outstanding probe so its late callback cannot
   count the same failure again or mutate a newer recovery.
3. Do not repeat warm-up after video preparation and per-consumer H264 handoff
   have already succeeded. Keep the existing SPS/PPS/keyframe checks.
4. Close WebRTC peers when offer creation, SDP exchange, or answer application
   fails, before retrying. Successful peers remain open; retry policy is unchanged.
5. Limit waiting for the global SDM command slot to the client's timeout
   (normally 45 seconds). Timed-out or canceled waiters never send their queued
   request. Concurrency remains one; this does not increase Google call capacity.
6. Use a Nest-only HTTP connection pool and close its idle connections after
   a command transport error. Other camera integrations retain their pools.
7. Decode at most 16 KiB of an HTTP error body. Report only allowlisted RPC
   statuses and recognized, fixed diagnostic labels. Never emit arbitrary API
   messages, response metadata, SDP, device IDs, or credentials from this parser.

Google documents `FAILED_PRECONDITION` for an unavailable camera, among other
errors. A bare HTTP 400 cannot identify that condition. See the
[official camera-stream error reference](https://developers.google.com/nest/device-access/traits/device/camera-live-stream#errors).

## Tests

- Before the implementation, the new publish regression test failed on v62:
  `successful publication rearmed publish backoff for 45s`.
- Focused Windows tests pass for nest, exec, streams, webrtc and rtsp.
- Linux race tests run in an isolated temporary source directory, with low
  process priority and constrained test parallelism. No production service is
  launched by these tests and no Google camera session is generated.
- Coverage includes late probe callbacks, preserved failed-probe cooldown,
  failed/successful peer lifecycle, bounded error parsing and redaction,
  canceled/expired command waiters, slot reuse, and transport cleanup.
- Full `internal/streams` testing fails the same pre-existing `TestRecursion`
  and `TestTempate` cases with `streams: source not supported`. They failed
  before edits and are explicitly excluded from the focused gate, not hidden
  or counted as passing. This is not a claim that the whole suite is green.
- Repeated Linux RTSP race testing also exposed `TestMissedControl` reporting
  an assertion from a goroutine after the test completed. It was reproduced
  on an untouched archive of v62. The focused Linux gate excludes that test
  as well; the RTSP production source was not edited.

## Limits and Deployment Gate

- This fixes reproduced local bugs, not the as-yet unidentified reason for
  an hour of Google 400 refusals. It cannot promise sub-minute recovery.
- The command admission bound is not a whole-camera recovery deadline. Actual
  request execution has its own timeout; explicit Google 429 cooldown and the
  existing generation/retry waits remain in force.
- The synchronous dial owner and idle-producer handoff still warrant separate
  investigation. The duplicate-warm-up correction reduces local delay, but
  does not replace these paths with a fully asynchronous state machine.
- Deployment requires a production backup and separate approval. Judge a
  later trial by finalized recording gaps and the time from first fresh media
  to resumed recording, not by green startup status. Preserve v62 for rollback.
- During a trial, verify repeated failures still back off, no stale callback
  resets a new recovery, audio remains AAC, and non-Nest streams are unchanged.
