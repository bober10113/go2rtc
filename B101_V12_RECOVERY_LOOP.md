# B101 v12 Local Recovery-Loop Test Notes

This note is sanitized. It intentionally avoids private camera names, local stream names, full device IDs, tokens, and credential URLs.

## Why v12 exists

v11 runtime logs showed the dominant failure was still local media recovery, not Google SDM extension timing. The pattern was repeated derived RTSP/ffmpeg restart loops with local RTSP `404`, `invalid data`, and timeout noise while the upstream Nest raw stream was recovering.

One important issue found in the v11 recovery path: duplicate upstream reset attempts were treated as handled resets, so every fast retry could extend the local recovery window again. That can turn a short recovery hold into a moving target.

## v12 changes

- Distinguish actual upstream Nest producer resets from duplicate resets already in progress.
- Do not extend the local Nest recovery window for duplicate reset attempts.
- Serialize derived `exec:` RTSP starts per local Nest input, reducing overlapping local ffmpeg start attempts during recovery.
- Clear local recovery state once a derived RTSP producer successfully publishes.
- Escalate the local recovery hold only after repeated actual resets in a short window.

## What to expect

Short recovery events may still happen. A good result is that recovery ends with a successful publish instead of repeated `404` / `invalid data` / ffmpeg restart loops. Google API status errors should remain low; if `400` or `429` storms return, that is a different signal and should be handled separately.
