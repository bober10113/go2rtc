# B101 v13 inactive Nest reset note

This test build keeps the v12 recovery-loop changes and narrows one case found
during startup/recovery testing.

When a derived local Nest RTSP command exits before publishing, go2rtc tries to
reset the upstream Nest producer. In v12, even an already-inactive Nest producer
could mark a full local recovery window. That delayed the next fresh session
attempt and left Frigate retrying the derived stream during the wait.

v13 treats an already-inactive Nest producer as handled but unchanged:

- no long local recovery window is created for an inactive producer
- active Nest producer resets still use the recovery window
- duplicate active resets still do not extend the recovery window
- the next derived-stream request can trigger a fresh Nest session sooner

Expected result: after restart or a local derived-stream crash, cameras should
avoid the extra one-minute hold caused by inactive upstream state. This is still
a test build and should be judged by Frigate/go2rtc logs over time.
