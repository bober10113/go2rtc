package exec

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/rtsp"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/magic"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	pkg "github.com/AlexxIT/go2rtc/pkg/rtsp"
	"github.com/AlexxIT/go2rtc/pkg/shell"
	"github.com/rs/zerolog"
)

func Init() {
	var cfg struct {
		Mod struct {
			AllowPaths []string `yaml:"allow_paths"`
		} `yaml:"exec"`
	}

	app.LoadConfig(&cfg)

	allowPaths = cfg.Mod.AllowPaths

	rtsp.HandleFunc(func(conn *pkg.Conn) bool {
		waitersMu.Lock()
		waiter := waiters[conn.URL.Path]
		waitersMu.Unlock()

		if waiter == nil {
			return false
		}

		// unblocking write to channel
		select {
		case waiter <- conn:
			return true
		default:
			return false
		}
	})

	streams.HandleFunc("exec", execHandle)
	streams.MarkInsecure("exec")

	log = app.GetLogger("exec")
}

var allowPaths []string

const (
	localNestRecoveryWindowBase   = 10 * time.Second
	localNestInactiveRecovery     = 10 * time.Second
	localNestRecoveryMedium       = 30 * time.Second
	localNestRecoveryLong         = time.Minute
	localNestRecoveryMax          = 2 * time.Minute
	localNestRecoveryRepeatWindow = 10 * time.Minute
	localNestStablePublishWindow  = 45 * time.Second
	localNestStartTimeout         = 90 * time.Second
	localNestRecoveryStartWaitMax = 10 * time.Second
)

var errLocalNestUpstreamReset = errors.New("exec: local nest upstream reset")

type localNestRecoveryState struct {
	until       time.Time
	failures    int
	lastFailure time.Time
	publishID   uint64
}

var localNestRecovery = struct {
	sync.Mutex
	state map[string]localNestRecoveryState
}{
	state: map[string]localNestRecoveryState{},
}

var localNestStartGates = struct {
	sync.Mutex
	gates map[string]*sync.Mutex
}{
	gates: map[string]*sync.Mutex{},
}

func execHandle(rawURL string) (prod core.Producer, err error) {
	rawURL, rawQuery, _ := strings.Cut(rawURL, "#")
	query := streams.ParseQuery(rawQuery)

	var path string

	// RTSP flow should have `{output}` inside URL
	// pipe flow may have `#{params}` inside URL
	if i := strings.Index(rawURL, "{output}"); i > 0 {
		if rtsp.Port == "" {
			return nil, errors.New("exec: rtsp module disabled")
		}

		sum := md5.Sum([]byte(rawURL))
		path = "/" + hex.EncodeToString(sum[:])
		rawURL = rawURL[:i] + "rtsp://127.0.0.1:" + rtsp.Port + path + rawURL[i+8:]
	}

	cmd := shell.NewCommand(rawURL[5:]) // remove `exec:`
	cmd.Stderr = &logWriter{
		buf:   make([]byte, 512),
		debug: log.Debug().Enabled(),
	}

	if allowPaths != nil && !slices.Contains(allowPaths, cmd.Args[0]) {
		_ = cmd.Close()
		return nil, errors.New("exec: bin not in allow_paths: " + cmd.Args[0])
	}

	if s := query.Get("killsignal"); s != "" {
		sig := syscall.Signal(core.Atoi(s))
		cmd.Cancel = func() error {
			log.Debug().Msgf("[exec] kill with signal=%d", sig)
			return cmd.Process.Signal(sig)
		}
	}

	if s := query.Get("killtimeout"); s != "" {
		cmd.WaitDelay = time.Duration(core.Atoi(s)) * time.Second
	}

	if query.Get("backchannel") == "1" {
		return pcm.NewBackchannel(cmd, query.Get("audio"))
	}

	var timeout time.Duration
	if s := query.Get("starttimeout"); s != "" {
		timeout = time.Duration(core.Atoi(s)) * time.Second
	} else {
		timeout = 30 * time.Second
	}

	if path == "" {
		prod, err = handlePipe(rawURL, cmd)
	} else {
		prod, err = handleRTSP(rawURL, cmd, path, timeout)
	}

	if err != nil {
		_ = cmd.Close()
	}

	return
}

func handlePipe(source string, cmd *shell.Command) (core.Producer, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	rd := struct {
		io.Reader
		io.Closer
	}{
		// add buffer for pipe reader to reduce syscall
		bufio.NewReaderSize(stdout, core.BufferSize),
		// stop cmd on close pipe call
		cmd,
	}

	log.Debug().Strs("args", cmd.Args).Msg("[exec] run pipe")

	ts := time.Now()

	if err = cmd.Start(); err != nil {
		return nil, err
	}

	prod, err := magic.Open(rd)
	if err != nil {
		return nil, fmt.Errorf("exec/pipe: %w\n%s", err, cmd.Stderr)
	}

	if info, ok := prod.(core.Info); ok {
		info.SetProtocol("pipe")
		setRemoteInfo(info, source, cmd.Args)
	}

	log.Debug().Stringer("launch", time.Since(ts)).Msg("[exec] run pipe")

	return prod, nil
}

func handleRTSP(source string, cmd *shell.Command, path string, timeout time.Duration) (core.Producer, error) {
	var localNestName string
	if name, ok := localNestInputName(cmd.Args); ok {
		localNestName = name
		gate := localNestStartGate(name)
		gate.Lock()
		defer gate.Unlock()

		if timeout < localNestStartTimeout {
			timeout = localNestStartTimeout
		}
		if err := waitLocalNestRecovery(name); err != nil {
			return nil, err
		}
	}

	if log.Trace().Enabled() {
		cmd.Stdout = os.Stdout
	}

	waiter := make(chan *pkg.Conn, 1)

	waitersMu.Lock()
	waiters[path] = waiter
	waitersMu.Unlock()

	defer func() {
		waitersMu.Lock()
		delete(waiters, path)
		waitersMu.Unlock()
	}()

	log.Debug().Strs("args", cmd.Args).Msg("[exec] run rtsp")

	ts := time.Now()

	if err := cmd.Start(); err != nil {
		log.Error().Err(err).Str("source", safeExecLogSource(source)).Msg("[exec]")
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		// haven't received data from app in timeout
		log.Error().Str("source", safeExecLogSource(source)).Msg("[exec] timeout")
		if resetLocalNestInput(cmd.Args, "exec start timeout") {
			return nil, errLocalNestUpstreamReset
		}
		return nil, errors.New("exec: timeout")
	case <-cmd.Done():
		// app fail before we receive any data
		if resetLocalNestInput(cmd.Args, "exec exited before publishing") {
			return nil, errLocalNestUpstreamReset
		}
		return nil, fmt.Errorf("exec/rtsp\n%s", cmd.Stderr)
	case prod := <-waiter:
		// app started successfully
		if localNestName != "" {
			markLocalNestPublished(localNestName, "exec published")
		}
		log.Debug().Stringer("launch", time.Since(ts)).Msg("[exec] run rtsp")
		setRemoteInfo(prod, source, cmd.Args)
		prod.OnClose = cmd.Close
		return prod, nil
	}
}

// internal

func resetLocalNestInput(args []string, reason string) bool {
	name, ok := localNestInputName(args)
	if !ok {
		return false
	}

	if handled, changed, inactive := streams.ResetIfSourceSchemeDetailed(name, "nest", reason); handled {
		if changed {
			wait := markLocalNestRecovery(name, reason, inactive)
			ev := log.Warn().
				Str("reason", reason).
				Stringer("wait", wait.Round(time.Millisecond))
			if inactive {
				ev.Msg("[exec] reset inactive upstream nest stream")
			} else {
				ev.Msg("[exec] reset upstream nest stream")
			}
		} else {
			log.Warn().Str("reason", reason).Msg("[exec] upstream nest reset already in progress")
		}
		return true
	}
	return false
}

func localNestInputName(args []string) (string, bool) {
	i := core.Index(args, "-i")
	if i <= 0 || i >= len(args)-1 {
		return "", false
	}

	u, err := url.Parse(args[i+1])
	if err != nil || u.Scheme != "rtsp" || u.Path == "" {
		return "", false
	}

	host := u.Hostname()
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return "", false
	}

	name := strings.TrimPrefix(u.Path, "/")
	if name == "" {
		return "", false
	}

	return name, true
}

func markLocalNestRecovery(name string, reason string, inactive bool) time.Duration {
	now := time.Now()

	localNestRecovery.Lock()
	st := localNestRecovery.state[name]
	if st.lastFailure.IsZero() || now.Sub(st.lastFailure) > localNestRecoveryRepeatWindow {
		st.failures = 0
	}
	st.failures++
	st.lastFailure = now

	wait := localNestRecoveryWindowBase
	if inactive {
		wait = localNestInactiveRecovery
	}
	switch {
	case st.failures >= 10:
		wait = localNestRecoveryMax
	case st.failures >= 6:
		wait = localNestRecoveryLong
	case st.failures >= 3:
		wait = localNestRecoveryMedium
	}
	st.until = now.Add(wait)
	localNestRecovery.state[name] = st
	localNestRecovery.Unlock()

	log.Warn().
		Str("reason", reason).
		Bool("inactive", inactive).
		Int("failures", st.failures).
		Bool("circuit_breaker", wait > localNestRecoveryWindowBase).
		Stringer("wait", wait.Round(time.Millisecond)).
		Msg("[exec] local nest recovery marked")

	return wait
}

func localNestRecoveryWait(name string) time.Duration {
	now := time.Now()

	localNestRecovery.Lock()
	st := localNestRecovery.state[name]
	if !st.until.IsZero() && !now.Before(st.until) {
		st.until = time.Time{}
		localNestRecovery.state[name] = st
	}
	localNestRecovery.Unlock()

	if wait := time.Until(st.until); wait > 0 {
		return wait
	}
	return 0
}

func waitLocalNestRecovery(name string) error {
	wait := localNestRecoveryWait(name)
	if wait <= 0 {
		return nil
	}

	if wait > localNestRecoveryStartWaitMax {
		log.Warn().
			Stringer("wait", wait.Round(time.Millisecond)).
			Msg("[exec] local nest upstream still recovering")
		return errLocalNestUpstreamReset
	}

	log.Warn().
		Stringer("wait", wait.Round(time.Millisecond)).
		Msg("[exec] waiting for local nest upstream recovery")
	timer := time.NewTimer(wait)
	defer timer.Stop()
	<-timer.C

	if wait = localNestRecoveryWait(name); wait > 0 {
		log.Warn().
			Stringer("wait", wait.Round(time.Millisecond)).
			Msg("[exec] local nest upstream still recovering")
		return errLocalNestUpstreamReset
	}
	log.Info().Msg("[exec] local nest recovery wait complete")
	return nil
}

func markLocalNestPublished(name string, reason string) {
	now := time.Now()

	localNestRecovery.Lock()
	st, ok := localNestRecovery.state[name]
	if !ok {
		localNestRecovery.Unlock()
		return
	}

	if st.failures < 2 {
		delete(localNestRecovery.state, name)
		localNestRecovery.Unlock()
		log.Info().Str("reason", reason).Msg("[exec] local nest upstream recovered")
		return
	}

	st.publishID++
	publishID := st.publishID
	st.until = now.Add(localNestStablePublishWindow)
	localNestRecovery.state[name] = st
	localNestRecovery.Unlock()

	log.Info().
		Str("reason", reason).
		Int("failures", st.failures).
		Stringer("probe", localNestStablePublishWindow).
		Msg("[exec] local nest upstream publish probe started")

	time.AfterFunc(localNestStablePublishWindow, func() {
		completeLocalNestPublishProbe(name, reason, publishID)
	})
}

func completeLocalNestPublishProbe(name string, reason string, publishID uint64) {
	localNestRecovery.Lock()
	st, ok := localNestRecovery.state[name]
	if ok && st.publishID == publishID && time.Since(st.lastFailure) >= localNestStablePublishWindow {
		delete(localNestRecovery.state, name)
	}
	localNestRecovery.Unlock()

	if ok && st.publishID == publishID && time.Since(st.lastFailure) >= localNestStablePublishWindow {
		log.Info().Str("reason", reason).Msg("[exec] local nest upstream recovered")
	}
}

func localNestStartGate(name string) *sync.Mutex {
	localNestStartGates.Lock()
	defer localNestStartGates.Unlock()

	gate := localNestStartGates.gates[name]
	if gate == nil {
		gate = &sync.Mutex{}
		localNestStartGates.gates[name] = gate
	}
	return gate
}

func safeExecLogSource(source string) string {
	if strings.Contains(source, "rtsp://127.0.0.1:") ||
		strings.Contains(source, "rtsp://localhost:") ||
		strings.Contains(source, "rtsp://[::1]:") {
		return "exec:<local rtsp source redacted>"
	}
	return source
}

var (
	log       zerolog.Logger
	waiters   = make(map[string]chan *pkg.Conn)
	waitersMu sync.Mutex
)

type logWriter struct {
	buf   []byte
	debug bool
	n     int
}

func (l *logWriter) String() string {
	if l.n == len(l.buf) {
		return string(l.buf) + "..."
	}
	return string(l.buf[:l.n])
}

func (l *logWriter) Write(p []byte) (n int, err error) {
	if l.n < cap(l.buf) {
		l.n += copy(l.buf[l.n:], p)
	}
	n = len(p)
	if l.debug {
		if p = trimSpace(p); p != nil {
			log.Debug().Msgf("[exec] %s", p)
		}
	}
	return
}

func trimSpace(b []byte) []byte {
	start := 0
	stop := len(b)
	for ; start < stop; start++ {
		if b[start] >= ' ' {
			break // trim all ASCII before 0x20
		}
	}
	for ; ; stop-- {
		if stop == start {
			return nil // skip empty output
		}
		if b[stop-1] > ' ' {
			break // trim all ASCII before 0x21
		}
	}
	return b[start:stop]
}

func setRemoteInfo(info core.Info, source string, args []string) {
	info.SetSource(source)

	if i := core.Index(args, "-i"); i > 0 && i < len(args)-1 {
		rawURL := args[i+1]
		if u, err := url.Parse(rawURL); err == nil && u.Host != "" {
			info.SetRemoteAddr(u.Host)
			info.SetURL(rawURL)
		}
	}
}
