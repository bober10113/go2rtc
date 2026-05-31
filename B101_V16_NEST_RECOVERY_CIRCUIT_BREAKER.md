# B101 v16 Nest recovery circuit breaker note

This test build keeps the v15 short recovery behavior for one-off Nest-derived
stream failures, but adds bounded escalation when the same local Nest-derived
stream repeatedly fails before publishing usable media.

Observed v15 behavior:

- The Google SDM command path can stay healthy while one derived RTSP stream
  loops through repeated local ffmpeg failures.
- A short fixed 10-second local recovery wait is not enough once a stream is in
  a persistent invalid-input/start-timeout loop.
- Repeated retries can make Frigate and go2rtc work harder without improving
  the chance of recovery.

v16 changes:

- Keep the first two failures on the fast 10-second recovery path.
- Escalate repeated failures for the same local Nest-derived input:
  - 3rd failure: 30 seconds
  - 6th failure: 1 minute
  - 10th failure and later: 2 minutes
- Mark escalated waits with `circuit_breaker=true` in logs.
- Clear the failure state when the derived stream publishes successfully.

Expected result:

- Normal transient drops should still recover quickly.
- A persistently failing derived Nest stream should stop hammering Frigate with
  rapid invalid-input ffmpeg restarts.
- This does not change Frigate config, camera config, audio handling, or Google
  stream-extension timing.
