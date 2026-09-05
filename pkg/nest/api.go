package nest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	nestOAuthTimeout      = 30 * time.Second
	nestGetDevicesTimeout = 30 * time.Second
	nestCommandTimeout    = 45 * time.Second
	nestStopTimeout       = 30 * time.Second

	nestExtendLeadTime   = 2 * time.Minute
	nestExtendJitterMax  = 2 * time.Minute
	nestExtendMinWait    = 30 * time.Second
	nestRetryJitterMax   = 15 * time.Second
	nestRateLimitBase    = 2 * time.Minute
	nestRateLimitMax     = 15 * time.Minute
	nestGenerateMinWait  = 60 * time.Second
	nestFailureWindow    = 5 * time.Minute
	nestFailureCooldown2 = 10 * time.Second
	nestFailureCooldown3 = 30 * time.Second
	nestFailureCooldownN = 60 * time.Second
)

type API struct {
	Token     string
	ExpiresAt time.Time

	ClientID     string
	ClientSecret string
	RefreshToken string

	StreamProjectID string
	StreamDeviceID  string
	StreamExpiresAt time.Time

	// WebRTC
	StreamSessionID string

	// RTSP
	StreamToken          string
	StreamExtensionToken string
	extendMu             sync.Mutex
	extendTimer          *time.Timer
	extendStop           chan struct{}
	extendOwner          *nestExtendOwner

	failureMu      sync.Mutex
	failureCount   int
	firstFailureAt time.Time
}

type Auth struct {
	AccessToken string
}

type DeviceInfo struct {
	Name      string
	DeviceID  string
	Protocols []string
}

var cache = map[string]*API{}
var cacheMu sync.Mutex

// commandGate serializes Google SDM executeCommand calls.
// This avoids several Nest cameras generating/extending/stopping at the same instant.
var commandGate = make(chan struct{}, 1)

// Nest transport recovery must not close idle connections used by other sources.
var nestHTTPTransport = http.DefaultTransport.(*http.Transport).Clone()

func newNestHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Transport: nestHTTPTransport, Timeout: timeout}
}

var rateLimitState = struct {
	sync.Mutex
	until time.Time
	count int
}{}

type nestExtendOwner struct {
	stop      chan struct{}
	closeOnce sync.Once
	deviceID  string
	sessionID string
}

func (o *nestExtendOwner) close() {
	if o == nil {
		return
	}
	o.closeOnce.Do(func() {
		close(o.stop)
	})
}

var nestExtendOwners = struct {
	sync.Mutex
	byDevice map[string]*nestExtendOwner
}{
	byDevice: map[string]*nestExtendOwner{},
}

type nestGenerateGate struct {
	sync.Mutex
	last time.Time
}

var nestGenerateGates = struct {
	sync.Mutex
	byDevice map[string]*nestGenerateGate
}{
	byDevice: map[string]*nestGenerateGate{},
}

type nestStatusError struct {
	Command    string
	StatusCode int
	Status     string
	RPCStatus  string
	Reason     string
}

func (e *nestStatusError) Error() string {
	text := "nest: wrong status: " + e.Status
	if e.RPCStatus != "" {
		text += " rpc=" + e.RPCStatus
	}
	if e.Reason != "" {
		text += " reason=" + e.Reason
	}
	return text
}

func newNestStatusError(command string, res *http.Response) error {
	err := &nestStatusError{
		Command:    command,
		StatusCode: res.StatusCode,
		Status:     res.Status,
	}
	if res.Body == nil {
		return err
	}
	const maxErrorBody = 16 * 1024
	body, readErr := io.ReadAll(io.LimitReader(res.Body, maxErrorBody+1))
	if readErr != nil || len(body) > maxErrorBody {
		return err
	}
	var envelope struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return err
	}
	// Do not log arbitrary API text: it can contain device IDs, SDP or tokens.
	switch envelope.Error.Status {
	case "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED",
		"NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED",
		"FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED",
		"INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED":
		err.RPCStatus = envelope.Error.Status
	}
	switch envelope.Error.Message {
	case "The camera is not available for streaming.":
		err.Reason = "camera_unavailable"
	case "Command is not supported for doorbell.":
		err.Reason = "unsupported_doorbell_command"
	case "Permission denied.":
		err.Reason = "permission_denied"
	}
	return err
}

func nestStatusCode(err error) int {
	var statusErr *nestStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode
	}
	return 0
}

func nestTerminalExtendStatus(err error) bool {
	switch nestStatusCode(err) {
	case http.StatusBadRequest, http.StatusNotFound:
		return true
	default:
		return false
	}
}

func nestRateLimitStatus(err error) bool {
	return nestStatusCode(err) == http.StatusTooManyRequests
}

func nestLogf(format string, args ...any) {
	log.Printf("[nest] "+format, args...)
}

func nestDeviceSuffix(deviceID string) string {
	if deviceID == "" {
		return "unknown"
	}
	if len(deviceID) <= 6 {
		return "..." + deviceID
	}
	return "..." + deviceID[len(deviceID)-6:]
}

func doNestRequest(client *http.Client, req *http.Request, command, deviceID string, attempt int) (*http.Response, error) {
	lockStart := time.Now()
	queueTimeout := client.Timeout
	if queueTimeout <= 0 {
		queueTimeout = nestCommandTimeout
	}
	queueCtx, cancel := context.WithTimeout(req.Context(), queueTimeout)
	defer cancel()
	select {
	case commandGate <- struct{}{}:
		defer func() { <-commandGate }()
	case <-queueCtx.Done():
		nestLogf("command queue expired command=%s device=%s attempt=%d wait=%s", command, nestDeviceSuffix(deviceID), attempt, time.Since(lockStart).Round(time.Millisecond))
		return nil, queueCtx.Err()
	}
	if err := queueCtx.Err(); err != nil {
		return nil, err
	}
	waitForNestRateLimit(command, deviceID)
	lockWait := time.Since(lockStart)

	started := time.Now()
	nestLogf("command start command=%s device=%s attempt=%d lock_wait=%s", command, nestDeviceSuffix(deviceID), attempt, lockWait.Round(time.Millisecond))
	res, err := client.Do(req)
	duration := time.Since(started)
	if err != nil {
		client.CloseIdleConnections()
		nestLogf("command error command=%s device=%s attempt=%d duration=%s error=%v", command, nestDeviceSuffix(deviceID), attempt, duration.Round(time.Millisecond), err)
		return nil, err
	}
	nestLogf("command done command=%s device=%s attempt=%d status=%d duration=%s", command, nestDeviceSuffix(deviceID), attempt, res.StatusCode, duration.Round(time.Millisecond))
	if res.StatusCode == http.StatusTooManyRequests {
		recordNestRateLimit(command, deviceID)
	}
	return res, nil
}

func waitForNestRateLimit(command, deviceID string) {
	rateLimitState.Lock()
	until := rateLimitState.until
	rateLimitState.Unlock()

	if wait := time.Until(until); wait > 0 {
		nestLogf("rate limit wait command=%s device=%s wait=%s", command, nestDeviceSuffix(deviceID), wait.Round(time.Millisecond))
		time.Sleep(wait)
	}
}

func recordNestRateLimit(command, deviceID string) {
	now := time.Now()

	rateLimitState.Lock()
	if now.After(rateLimitState.until) {
		rateLimitState.count = 0
	}
	rateLimitState.count++
	count := rateLimitState.count

	cooldown := nestRateLimitBase
	for i := 1; i < count && cooldown < nestRateLimitMax; i++ {
		cooldown *= 2
	}
	if cooldown > nestRateLimitMax {
		cooldown = nestRateLimitMax
	}
	cooldown += nestJitter(deviceID, nestRetryJitterMax)

	until := now.Add(cooldown)
	if until.After(rateLimitState.until) {
		rateLimitState.until = until
	}
	rateLimitState.Unlock()

	nestLogf("rate limit cooldown command=%s device=%s wait=%s", command, nestDeviceSuffix(deviceID), cooldown.Round(time.Millisecond))
}

func registerNestExtendOwner(owner *nestExtendOwner) {
	if owner == nil || owner.deviceID == "" {
		return
	}

	nestExtendOwners.Lock()
	previous := nestExtendOwners.byDevice[owner.deviceID]
	if previous != nil && previous != owner {
		previous.close()
		nestLogf("extend superseded device=%s", nestDeviceSuffix(owner.deviceID))
	}
	nestExtendOwners.byDevice[owner.deviceID] = owner
	nestExtendOwners.Unlock()
}

func unregisterNestExtendOwner(owner *nestExtendOwner) {
	if owner == nil || owner.deviceID == "" {
		return
	}

	nestExtendOwners.Lock()
	if nestExtendOwners.byDevice[owner.deviceID] == owner {
		delete(nestExtendOwners.byDevice, owner.deviceID)
	}
	nestExtendOwners.Unlock()
}

func nestGenerateGateFor(deviceID string) *nestGenerateGate {
	nestGenerateGates.Lock()
	gate := nestGenerateGates.byDevice[deviceID]
	if gate == nil {
		gate = new(nestGenerateGate)
		nestGenerateGates.byDevice[deviceID] = gate
	}
	nestGenerateGates.Unlock()

	return gate
}

func beginNestGenerate(deviceID string) func() {
	gate := nestGenerateGateFor(deviceID)
	gate.Lock()

	if wait := time.Until(gate.last.Add(nestGenerateMinWait)); wait > 0 {
		nestLogf("generate wait device=%s wait=%s", nestDeviceSuffix(deviceID), wait.Round(time.Millisecond))
		time.Sleep(wait)
	}

	gate.last = time.Now()
	return gate.Unlock
}

func (a *API) clearExtendOwner(owner *nestExtendOwner) {
	a.extendMu.Lock()
	if a.extendOwner == owner {
		a.extendOwner = nil
		a.extendStop = nil
		a.extendTimer = nil
	}
	a.extendMu.Unlock()

	unregisterNestExtendOwner(owner)
}

func (a *API) clearTerminalStreamSession(owner *nestExtendOwner) {
	a.extendMu.Lock()
	if a.extendOwner == owner {
		a.extendOwner = nil
		a.extendStop = nil
		a.extendTimer = nil
		a.StreamSessionID = ""
		a.StreamToken = ""
		a.StreamExtensionToken = ""
		a.StreamExpiresAt = time.Time{}
	}
	a.extendMu.Unlock()

	unregisterNestExtendOwner(owner)
}

func (a *API) CloneForStream() *API {
	return &API{
		Token:        a.Token,
		ExpiresAt:    a.ExpiresAt,
		ClientID:     a.ClientID,
		ClientSecret: a.ClientSecret,
		RefreshToken: a.RefreshToken,
	}
}

func nestJitter(deviceID string, max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	var n int64
	for _, ch := range deviceID {
		n += int64(ch)
	}
	return time.Duration(n % int64(max))
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (a *API) recordCommandFailure(command string, err error) time.Duration {
	now := time.Now()
	a.failureMu.Lock()
	if a.firstFailureAt.IsZero() || now.Sub(a.firstFailureAt) > nestFailureWindow {
		a.firstFailureAt = now
		a.failureCount = 0
	}
	a.failureCount++
	count := a.failureCount

	var cooldown time.Duration
	switch {
	case count <= 1:
		cooldown = 0
	case count == 2:
		cooldown = nestFailureCooldown2 + nestJitter(a.StreamDeviceID, nestRetryJitterMax)
	case count == 3:
		cooldown = nestFailureCooldown3 + nestJitter(a.StreamDeviceID, nestRetryJitterMax)
	default:
		cooldown = nestFailureCooldownN + nestJitter(a.StreamDeviceID, nestRetryJitterMax)
	}
	a.failureMu.Unlock()

	nestLogf("command failure command=%s device=%s failures=%d cooldown=%s error=%v", command, nestDeviceSuffix(a.StreamDeviceID), count, cooldown.Round(time.Millisecond), err)
	return cooldown
}

func (a *API) recordCommandSuccess(command string) {
	a.failureMu.Lock()
	count := a.failureCount
	a.failureCount = 0
	a.firstFailureAt = time.Time{}
	a.failureMu.Unlock()
	if count > 0 {
		nestLogf("command recovered command=%s device=%s previous_failures=%d", command, nestDeviceSuffix(a.StreamDeviceID), count)
	}
}

func (a *API) sleepAfterFailure(command string, err error, base time.Duration) {
	cooldown := a.recordCommandFailure(command, err)
	wait := maxDuration(base, cooldown)
	if wait <= 0 {
		return
	}
	nestLogf("retry backoff command=%s device=%s wait=%s", command, nestDeviceSuffix(a.StreamDeviceID), wait.Round(time.Millisecond))
	time.Sleep(wait)
}

func NewAPI(clientID, clientSecret, refreshToken string) (*API, error) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	key := clientID + ":" + clientSecret + ":" + refreshToken
	now := time.Now()

	if api := cache[key]; api != nil && now.Before(api.ExpiresAt) {
		return api.CloneForStream(), nil
	}

	data := url.Values{
		"grant_type":    []string{"refresh_token"},
		"client_id":     []string{clientID},
		"client_secret": []string{clientSecret},
		"refresh_token": []string{refreshToken},
	}

	client := newNestHTTPClient(nestOAuthTimeout)
	res, err := client.PostForm("https://www.googleapis.com/oauth2/v4/token", data)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, newNestStatusError("NewAPI", res)
	}

	var resv struct {
		AccessToken string        `json:"access_token"`
		ExpiresIn   time.Duration `json:"expires_in"`
		Scope       string        `json:"scope"`
		TokenType   string        `json:"token_type"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return nil, err
	}

	api := &API{
		Token:        resv.AccessToken,
		ExpiresAt:    now.Add(resv.ExpiresIn * time.Second),
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RefreshToken: refreshToken,
	}

	cache[key] = api

	return api.CloneForStream(), nil
}

func (a *API) GetDevices(projectID string) ([]DeviceInfo, error) {
	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" + projectID + "/devices"
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := newNestHTTPClient(nestGetDevicesTimeout)
	res, err := doNestRequest(client, req, "GetDevices", "", 1)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, newNestStatusError("GetDevices", res)
	}

	var resv struct {
		Devices []Device
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return nil, err
	}

	devices := make([]DeviceInfo, 0, len(resv.Devices))

	for _, device := range resv.Devices {
		// only RTSP and WEB_RTC available (both supported)
		if len(device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols) == 0 {
			continue
		}

		i := strings.LastIndexByte(device.Name, '/')
		if i <= 0 {
			continue
		}

		name := device.Traits.SdmDevicesTraitsInfo.CustomName
		// Devices configured through the Nest app use the container/room name as opposed to the customName trait
		if name == "" && len(device.ParentRelations) > 0 {
			name = device.ParentRelations[0].DisplayName
		}

		devices = append(devices, DeviceInfo{
			Name:      name,
			DeviceID:  device.Name[i+1:],
			Protocols: device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols,
		})
	}

	return devices, nil
}

func (a *API) ExchangeSDP(projectID, deviceID, offer string) (string, error) {
	command := "GenerateWebRtcStream"
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			Offer string `json:"offerSdp"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream"
	reqv.Params.Offer = offer

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"

	endGenerate := beginNestGenerate(deviceID)
	defer endGenerate()

	maxRetries := 3
	retryDelay := time.Second * 30

	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
		if err != nil {
			return "", err
		}

		req.Header.Set("Authorization", "Bearer "+a.Token)

		client := newNestHTTPClient(nestCommandTimeout)
		res, err := doNestRequest(client, req, command, deviceID, attempt)
		if err != nil {
			if attempt < maxRetries {
				a.sleepAfterFailure(command, err, retryDelay)
				retryDelay *= 2
				continue
			}
			return "", err
		}

		switch res.StatusCode {
		case http.StatusUnauthorized:
			err := newNestStatusError(command, res)
			res.Body.Close()
			if attempt < maxRetries {
				nestLogf("token refresh start command=%s device=%s attempt=%d", command, nestDeviceSuffix(deviceID), attempt)
				if err := a.refreshToken(); err != nil {
					return "", err
				}
				nestLogf("token refresh done command=%s device=%s attempt=%d", command, nestDeviceSuffix(deviceID), attempt)
				a.sleepAfterFailure(command, err, time.Second)
				continue
			}
			return "", err
		case http.StatusConflict, http.StatusTooManyRequests:
			err := newNestStatusError(command, res)
			res.Body.Close()
			if attempt < maxRetries {
				a.sleepAfterFailure(command, err, retryDelay)
				retryDelay *= 2
				continue
			}
			return "", err
		}

		if res.StatusCode != http.StatusOK {
			err := newNestStatusError(command, res)
			res.Body.Close()
			return "", err
		}

		var resv struct {
			Results struct {
				Answer         string    `json:"answerSdp"`
				ExpiresAt      time.Time `json:"expiresAt"`
				MediaSessionID string    `json:"mediaSessionId"`
			} `json:"results"`
		}

		if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
			res.Body.Close()
			return "", err
		}
		res.Body.Close()

		a.StreamProjectID = projectID
		a.StreamDeviceID = deviceID
		a.StreamSessionID = resv.Results.MediaSessionID
		a.StreamExpiresAt = resv.Results.ExpiresAt
		a.recordCommandSuccess(command)
		nestLogf("session generated command=%s device=%s expires=%s", command, nestDeviceSuffix(deviceID), a.StreamExpiresAt.UTC().Format(time.RFC3339))

		return resv.Results.Answer, nil
	}

	return "", errors.New("nest: max retries exceeded")
}

func (a *API) refreshToken() error {
	if a.ClientID == "" || a.ClientSecret == "" || a.RefreshToken == "" {
		return errors.New("nest: missing cached credentials for token refresh")
	}

	key := a.ClientID + ":" + a.ClientSecret + ":" + a.RefreshToken

	cacheMu.Lock()
	delete(cache, key)
	cacheMu.Unlock()

	newAPI, err := NewAPI(a.ClientID, a.ClientSecret, a.RefreshToken)
	if err != nil {
		return err
	}

	a.Token = newAPI.Token
	a.ExpiresAt = newAPI.ExpiresAt
	return nil
}

func (a *API) ExtendStream() error {
	command := "ExtendWebRtcStream"
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			MediaSessionID       string `json:"mediaSessionId,omitempty"`
			StreamExtensionToken string `json:"streamExtensionToken,omitempty"`
		} `json:"params"`
	}

	if a.StreamToken != "" {
		// RTSP
		command = "ExtendRtspStream"
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendRtspStream"
		reqv.Params.StreamExtensionToken = a.StreamExtensionToken
	} else {
		// WebRTC
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendWebRtcStream"
		reqv.Params.MediaSessionID = a.StreamSessionID
	}

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"

	maxRetries := 3
	retryDelay := time.Second * 10

	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
		if err != nil {
			return err
		}

		req.Header.Set("Authorization", "Bearer "+a.Token)

		client := newNestHTTPClient(nestCommandTimeout)
		res, err := doNestRequest(client, req, command, a.StreamDeviceID, attempt)
		if err != nil {
			if attempt < maxRetries {
				a.sleepAfterFailure(command, err, retryDelay)
				retryDelay *= 2
				continue
			}
			return err
		}

		switch res.StatusCode {
		case http.StatusUnauthorized:
			err := newNestStatusError(command, res)
			res.Body.Close()
			if attempt < maxRetries {
				nestLogf("token refresh start command=%s device=%s attempt=%d", command, nestDeviceSuffix(a.StreamDeviceID), attempt)
				if err := a.refreshToken(); err != nil {
					return err
				}
				nestLogf("token refresh done command=%s device=%s attempt=%d", command, nestDeviceSuffix(a.StreamDeviceID), attempt)
				a.sleepAfterFailure(command, err, time.Second)
				continue
			}
			return err
		case http.StatusConflict, http.StatusTooManyRequests:
			err := newNestStatusError(command, res)
			res.Body.Close()
			if attempt < maxRetries {
				a.sleepAfterFailure(command, err, retryDelay)
				retryDelay *= 2
				continue
			}
			return err
		}

		if res.StatusCode != http.StatusOK {
			err := newNestStatusError(command, res)
			res.Body.Close()
			return err
		}

		var resv struct {
			Results struct {
				ExpiresAt            time.Time `json:"expiresAt"`
				MediaSessionID       string    `json:"mediaSessionId"`
				StreamExtensionToken string    `json:"streamExtensionToken"`
				StreamToken          string    `json:"streamToken"`
			} `json:"results"`
		}

		if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
			res.Body.Close()
			return err
		}
		res.Body.Close()

		a.StreamSessionID = resv.Results.MediaSessionID
		a.StreamExpiresAt = resv.Results.ExpiresAt
		a.StreamExtensionToken = resv.Results.StreamExtensionToken
		a.StreamToken = resv.Results.StreamToken
		a.recordCommandSuccess(command)
		nestLogf("session extended command=%s device=%s expires=%s", command, nestDeviceSuffix(a.StreamDeviceID), a.StreamExpiresAt.UTC().Format(time.RFC3339))

		return nil
	}

	return errors.New("nest: max retries exceeded")
}

func (a *API) GenerateRtspStream(projectID, deviceID string) (string, error) {
	command := "GenerateRtspStream"
	var reqv struct {
		Command string   `json:"command"`
		Params  struct{} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateRtspStream"

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := newNestHTTPClient(nestCommandTimeout)
	res, err := doNestRequest(client, req, command, deviceID, 1)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", newNestStatusError(command, res)
	}

	var resv struct {
		Results struct {
			StreamURLs           map[string]string `json:"streamUrls"`
			StreamExtensionToken string            `json:"streamExtensionToken"`
			StreamToken          string            `json:"streamToken"`
			ExpiresAt            time.Time         `json:"expiresAt"`
		} `json:"results"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return "", err
	}

	if _, ok := resv.Results.StreamURLs["rtspUrl"]; !ok {
		return "", errors.New("nest: failed to generate rtsp url")
	}

	a.StreamProjectID = projectID
	a.StreamDeviceID = deviceID
	a.StreamToken = resv.Results.StreamToken
	a.StreamExtensionToken = resv.Results.StreamExtensionToken
	a.StreamExpiresAt = resv.Results.ExpiresAt
	a.recordCommandSuccess(command)
	nestLogf("session generated command=%s device=%s expires=%s", command, nestDeviceSuffix(deviceID), a.StreamExpiresAt.UTC().Format(time.RFC3339))

	return resv.Results.StreamURLs["rtspUrl"], nil
}

func (a *API) StopRTSPStream() error {
	command := "StopRtspStream"
	if a.StreamProjectID == "" || a.StreamDeviceID == "" {
		return errors.New("nest: tried to stop rtsp stream without a project or device ID")
	}

	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			StreamExtensionToken string `json:"streamExtensionToken"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.StopRtspStream"
	reqv.Params.StreamExtensionToken = a.StreamExtensionToken

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := newNestHTTPClient(nestStopTimeout)
	res, err := doNestRequest(client, req, command, a.StreamDeviceID, 1)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return newNestStatusError(command, res)
	}

	nestLogf("session stopped command=%s device=%s", command, nestDeviceSuffix(a.StreamDeviceID))
	a.StreamProjectID = ""
	a.StreamDeviceID = ""
	a.StreamExtensionToken = ""
	a.StreamToken = ""

	return nil
}

type Device struct {
	Name string `json:"name"`
	Type string `json:"type"`
	//Assignee string `json:"assignee"`
	Traits struct {
		SdmDevicesTraitsInfo struct {
			CustomName string `json:"customName"`
		} `json:"sdm.devices.traits.Info"`
		SdmDevicesTraitsCameraLiveStream struct {
			VideoCodecs        []string `json:"videoCodecs"`
			AudioCodecs        []string `json:"audioCodecs"`
			SupportedProtocols []string `json:"supportedProtocols"`
		} `json:"sdm.devices.traits.CameraLiveStream"`
		//SdmDevicesTraitsCameraImage struct {
		//	MaxImageResolution struct {
		//		Width  int `json:"width"`
		//		Height int `json:"height"`
		//	} `json:"maxImageResolution"`
		//} `json:"sdm.devices.traits.CameraImage"`
		//SdmDevicesTraitsCameraPerson struct {
		//} `json:"sdm.devices.traits.CameraPerson"`
		//SdmDevicesTraitsCameraMotion struct {
		//} `json:"sdm.devices.traits.CameraMotion"`
		//SdmDevicesTraitsDoorbellChime struct {
		//} `json:"sdm.devices.traits.DoorbellChime"`
		//SdmDevicesTraitsCameraClipPreview struct {
		//} `json:"sdm.devices.traits.CameraClipPreview"`
	} `json:"traits"`
	ParentRelations []struct {
		Parent      string `json:"parent"`
		DisplayName string `json:"displayName"`
	} `json:"parentRelations"`
}

func (a *API) StartExtendStreamTimer() {
	a.extendMu.Lock()
	if a.extendOwner != nil {
		a.extendMu.Unlock()
		return
	}

	owner := &nestExtendOwner{
		stop:      make(chan struct{}),
		deviceID:  a.StreamDeviceID,
		sessionID: a.StreamSessionID,
	}
	a.extendOwner = owner
	a.extendStop = owner.stop
	a.extendMu.Unlock()

	registerNestExtendOwner(owner)

	go func() {
		defer a.clearExtendOwner(owner)

		for {
			wait := time.Until(a.StreamExpiresAt) - nestExtendLeadTime - nestJitter(a.StreamDeviceID, nestExtendJitterMax)
			if wait < nestExtendMinWait {
				wait = nestExtendMinWait + nestJitter(a.StreamDeviceID, nestRetryJitterMax)
			}
			nestLogf("extend scheduled device=%s wait=%s expires=%s", nestDeviceSuffix(a.StreamDeviceID), wait.Round(time.Millisecond), a.StreamExpiresAt.UTC().Format(time.RFC3339))

			timer := time.NewTimer(wait)
			a.extendMu.Lock()
			if a.extendOwner == owner {
				a.extendTimer = timer
			}
			a.extendMu.Unlock()

			select {
			case <-timer.C:
				if err := a.ExtendStream(); err != nil {
					backoff := nestFailureCooldown3 + nestJitter(a.StreamDeviceID, nestRetryJitterMax)
					if nestTerminalExtendStatus(err) {
						nestLogf("extend terminal device=%s status=%d action=stop error=%v", nestDeviceSuffix(a.StreamDeviceID), nestStatusCode(err), err)
						a.clearTerminalStreamSession(owner)
						return
					}
					if nestRateLimitStatus(err) {
						backoff = nestRateLimitBase + nestJitter(a.StreamDeviceID, nestRetryJitterMax)
					}
					nestLogf("extend failed device=%s backoff=%s error=%v", nestDeviceSuffix(a.StreamDeviceID), backoff.Round(time.Millisecond), err)
					select {
					case <-time.After(backoff):
						continue
					case <-owner.stop:
						return
					}
				}
			case <-owner.stop:
				timer.Stop()
				nestLogf("extend stopped device=%s", nestDeviceSuffix(a.StreamDeviceID))
				return
			}
		}
	}()
}

func (a *API) StopExtendStreamTimer() {
	a.extendMu.Lock()
	owner := a.extendOwner
	if owner != nil {
		owner.close()
		a.extendOwner = nil
		a.extendStop = nil
	}
	if a.extendTimer != nil {
		a.extendTimer.Stop()
		a.extendTimer = nil
	}
	a.extendMu.Unlock()

	unregisterNestExtendOwner(owner)
}
