# B101 v15 short local recovery wait note

This test build keeps the v14 inactive-producer handling and shortens the active local Nest recovery path.

Observed v14 behavior:

- A local derived Nest ffmpeg/restream path can time out while the Google/Nest API session extension path stays healthy.
- go2rtc marks local recovery and can regenerate a Nest session successfully.
- Frigate retries the local RTSP stream during the local recovery window and may log repeated invalid-data / ffmpeg restart noise.

v15 changes:

- Local Nest recovery waits are kept short at 10 seconds for both active and inactive upstream reset cases.
- The longer local 60-second, 2-minute, and 3-minute holds are removed from this test build.
- If a retry arrives during a short recovery wait, the exec handler waits through that short gate while holding the per-input start lock, then tries to publish the derived stream instead of immediately returning a local reset error.

Expected result: recovery should still avoid immediate retry storms, but Frigate should no longer be blocked by a local recovery gate for 1-3 minutes after a fresh Nest session has already been generated.

Runtime note: initial Frigate/ffmpeg invalid-data restarts may still happen if the derived RTSP stream is not ready to deliver usable media. The next test area is derived-stream media readiness before exposing the stream back to Frigate.
