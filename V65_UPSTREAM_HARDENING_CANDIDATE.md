# v65 candidate: targeted upstream hardening

Prepared November 18, 2026. Deployed with explicit approval at 22:29:42 EST
on the same date. Initial recording/playback verification passed; long-term
stability remains unproven. The candidate-stage notes below are historical.
Base: v64 source `a19816c` plus documentation commit `650900d`.
No Nest recovery policy, media codecs, dependency versions or Frigate settings
are changed. This is reliability/security work, not a measured CPU optimization.

## Changes

1. Import upstream WriteBuffer disconnect fix `6e1a526` and follow-up `8e36b29`
   ([PR 2339](https://github.com/AlexxIT/go2rtc/pull/2339)). Prevent late writes
   after an HTTP response writer is released. Add a local correction for Close
   before WriteTo: a late consumer must return promptly without flushing stale
   buffered media. This regression failed against the imported fix before the
   local correction and now passes.
2. Import constant-time auth comparison `68b05ea` and `add2d63`
   ([PR 2693](https://github.com/AlexxIT/go2rtc/pull/2693)). Locally retain Go's
   BasicAuth parsing and compare fixed-size username/password hashes, evaluating
   both comparisons. The upstream full-header comparison rejected lowercase
   `basic`, which the old middleware accepted; a regression test reproduced it.
   Loopback bypass and local_auth policy are unchanged.
3. Import Tapo backchannel/read ownership guard `0d66fb1`
   ([PR 2691](https://github.com/AlexxIT/go2rtc/pull/2691)). Reject stream setup
   after the backchannel reader has started instead of competing for replies.
   Existing sessions remain usable. The regression test uses a reader-start
   signal, not a timing-dependent sleep, and closes its test connections.
4. Repair stale core/creds/streams tests. Credential-redaction tests now call the
   current creds implementation. Configured-source fixtures use NewStream as
   production config loading does, instead of the restricted dynamic-source API.
   Stream fixtures restore global state. Production validation is unchanged.

## Verification

- Windows Go 1.26.3: ten repetitions of core, creds, tapo, tcp, internal/api,
  internal/streams, internal/exec, nest and webrtc passed.
- Linux Go 1.24 container: the same nine packages passed with `-race -count=10`.
- Linux WriteBuffer regression tests passed with `-race -count=100`.
- Targeted `go vet` passes for all listed packages except the unchanged WebRTC
  package, which has pre-existing unkeyed-literal and unreachable-code warnings;
  these were reproduced on the untouched v64 checkout.
- Full `go test ./...` was attempted on Windows and Linux. It is NOT green.

Remaining full-suite failures were also reproduced on untouched v64 on Windows:
FFmpeg x265 argument expectations, AAC uint16-vs-uint8 assertions, removed HomeKit
SetupEndpoints/Client symbols, HomeKit TLV expectations, and nonconstant
fmt.Errorf format strings in ivideon/Tuya. The isolated Linux run additionally
exposes OS-specific device-argument expectations and unavailable multicast
interfaces under `--network none`. None of those packages' production code is
modified by this candidate. Do not describe the entire suite as passing.

Linux checks ran in a one-CPU, 2 GB limited container with no external network,
read-only source and module cache, no camera configuration and no production
ports. Tests establish synthetic behavior only, not real camera acceptance.

## Expected benefit and limits

The buffer fixes can prevent disconnect-related HTTP live-view failures; the Tapo
guard addresses a specific two-way-talk deadlock. Authentication hardening is a
security improvement. None proves a fix for wireless packet loss, Google-side
stalls or all long Nest recording gaps. v64's existing Nest recovery policy stays
intact; do not claim a new recovery deadline.

## Build receipt

- Source commit: `89d4b065556a5a681828139a9eafe26e7073d6bd`.
- Linux amd64, Go 1.24.13, CGO disabled, `go build -p=2 -trimpath`.
- Binary: `go2rtc-b101-v65-candidate-linux-amd64`.
- SHA-256: `eeac628a1ea0b8677d3ff3f7923f678f1fbcc7ccf2e1ee9e62f373b3c7bff0c6`.
- Executed only with `-version` in an isolated Linux container:
  `go2rtc version 1.9.14+dev.89d4b06 (89d4b06) linux/amd64`.
- The same nine targeted packages also passed 25 shuffled-order repetitions
  on Windows. Source and local candidate artifact are preserved; no production
  installation or release-binary publication was performed.

The build receipt is a documentation-only follow-up to the source commit above.

## Deployment gate

Do not install automatically. Preserve the currently installed binary and config,
then use a separately authorized deployment with a single restart. Verify the
running binary hash, one process, all cameras' fresh finalized recordings and
live playback. Compare recording gaps and failure markers over a longer window.
Ask before reverting. Keep private configuration/logs out of the public branch.

## Deployment receipt - November 18, 2026

- Installed the exact source-commit `89d4b06` binary and checksum recorded above.
- One Frigate restart; Frigate 0.18.3 image, persistent configuration, Compose,
  command allowlist and container host settings remained unchanged.
- Backed up the previous binary and deployment files before atomic replacement.
- Verified one running go2rtc process with the expected v65 checksum.
- All eight enabled cameras produced finalized recordings within the initial
  check. All eight recording HLS playlists and media segments loaded; a Nest and
  non-Nest sample decoded successfully.
- Planned restart gaps: 22-24 seconds for non-Nest, 36-45 seconds for Nest.
- No checked startup FFmpeg crash, missing-frame, invalid-data, media-stall,
  panic, restricted-source or nginx uninitialized-variable errors.
- Authentication remained enforced. This is initial verification only, not
  proof of long-term stability or a guarantee about future network outages.
