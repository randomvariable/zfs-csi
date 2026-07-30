package nfsexport

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type manualRootTimer struct {
	duration time.Duration
	ch       chan time.Time
	stopped  chan struct{}
	once     sync.Once
	stopOnce sync.Once
	stopHook func()
}

func (t *manualRootTimer) C() <-chan time.Time { return t.ch }
func (t *manualRootTimer) Stop() bool {
	t.stopOnce.Do(func() {
		close(t.stopped)
		if t.stopHook != nil {
			t.stopHook()
		}
	})
	return true
}
func (t *manualRootTimer) fire() {
	t.once.Do(func() { t.ch <- time.Time{} })
}

type manualRootTimers struct {
	created chan *manualRootTimer
}

func newManualRootTimers() *manualRootTimers {
	return &manualRootTimers{created: make(chan *manualRootTimer, 16)}
}

func (f *manualRootTimers) New(duration time.Duration) rootTimer {
	timer := &manualRootTimer{duration: duration, ch: make(chan time.Time, 1), stopped: make(chan struct{})}
	f.created <- timer
	return timer
}

func (f *manualRootTimers) next(t *testing.T, want time.Duration) *manualRootTimer {
	t.Helper()
	select {
	case timer := <-f.created:
		if timer.duration != want {
			t.Fatalf("timer duration = %v, want %v", timer.duration, want)
		}
		return timer
	case <-t.Context().Done():
		t.Fatal("timed out waiting for root timer")
		return nil
	}
}

type scriptedRootWriter struct {
	mu       sync.Mutex
	outcomes []error
	attempts chan Entry
}

func (w *scriptedRootWriter) InstallRootPositive(entry Entry) error {
	w.attempts <- entry
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.outcomes) == 0 {
		return nil
	}
	err := w.outcomes[0]
	w.outcomes = w.outcomes[1:]
	return err
}

func (w *scriptedRootWriter) nextAttempt(t *testing.T) Entry {
	t.Helper()
	select {
	case entry := <-w.attempts:
		return entry
	case <-t.Context().Done():
		t.Fatal("timed out waiting for root install attempt")
		return Entry{}
	}
}

func newTestRootController(writer rootPositiveWriter, timers *manualRootTimers) *RootController {
	c := newRootController(writer, nil)
	c.newTimer = timers.New
	c.retryInitial = 10 * time.Millisecond
	c.retryMax = 25 * time.Millisecond
	c.refreshInterval = time.Minute
	c.jitter = func(delay time.Duration) time.Duration { return delay }
	return c
}

func rootBodyError(errno error) error {
	return &cacheWriteError{channel: ChannelExport, phase: cacheWriteBody, err: errno}
}

func rootOpenError(errno error) error {
	return &cacheWriteError{channel: ChannelExport, phase: cacheWriteOpen, err: errno}
}

func startRootController(t *testing.T, controller *RootController) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		controller.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel
}

func TestRootControllerRetryableENOENTUsesBoundedBackoffAndResetsAfterSuccess(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{
			rootBodyError(unix.ENOENT),
			rootBodyError(unix.EAGAIN),
			rootBodyError(unix.EIO),
			nil,
			rootBodyError(unix.ENOENT),
		},
		attempts: make(chan Entry, 8),
	}
	controller := newTestRootController(writer, timers)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)

	for _, want := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond} {
		if got := writer.nextAttempt(t).Path; got != "/tank" {
			t.Fatalf("attempt path = %q", got)
		}
		timers.next(t, want).fire()
	}
	writer.nextAttempt(t)
	timers.next(t, time.Minute)

	controller.Kick()
	writer.nextAttempt(t)
	timers.next(t, 10*time.Millisecond)
}

func TestRootControllerLostWakeupStillConvergesByTimer(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT), nil},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)

	writer.nextAttempt(t)
	timers.next(t, 10*time.Millisecond).fire()
	writer.nextAttempt(t)
	timers.next(t, time.Minute)
}

func TestRootControllerRetrySuccessRefreshesIdempotently(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT), nil, nil},
		attempts: make(chan Entry, 3),
	}
	controller := newTestRootController(writer, timers)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)

	writer.nextAttempt(t)
	timers.next(t, 10*time.Millisecond).fire()
	writer.nextAttempt(t)
	timers.next(t, time.Minute).fire()
	writer.nextAttempt(t)
	timers.next(t, time.Minute)
}

func TestRootControllerRepeatedEventsCoalesce(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{attempts: make(chan Entry, 4)}
	controller := newTestRootController(writer, timers)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		controller.Kick()
	}
	startRootController(t, controller)

	writer.nextAttempt(t)
	timers.next(t, time.Minute)
	if got := len(writer.attempts); got != 0 {
		t.Fatalf("coalesced events caused %d extra attempts", got)
	}
}

func TestRootControllerShutdownCancelsRetryAndWaitsCleanly(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT)},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	cancel := startRootController(t, controller)
	writer.nextAttempt(t)
	timer := timers.next(t, 10*time.Millisecond)
	cancel()
	select {
	case <-timer.stopped:
	case <-t.Context().Done():
		t.Fatal("shutdown left retry timer active")
	}
	if got := len(writer.attempts); got != 0 {
		t.Fatalf("shutdown allowed %d retry attempts", got)
	}
}

func TestRootControllerPermanentErrorHasNoRetryTimerAndNewDesiredGenerationRetries(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.EINVAL)},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)
	writer.nextAttempt(t)
	if got := len(timers.created); got != 0 {
		t.Fatalf("terminal failure scheduled %d retry timers", got)
	}
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	writer.nextAttempt(t)
	timers.next(t, time.Minute)
}

func TestRootControllerRejectsMalformedOrConflictingDesiredRoot(t *testing.T) {
	controller := newRootController(&scriptedRootWriter{attempts: make(chan Entry, 1)}, nil)
	for _, entry := range []Entry{
		{Path: "relative", Root: true},
		{Path: "/tank/../other", Root: true},
		{Path: "/tank", Root: false},
		{Path: "/tank", Root: true},
		{Path: "/tank", Root: true, AccessMode: AccessRW},
		{Path: "/tank", Root: true, AccessMode: AccessRO, TLS: true},
		{Path: "/tank", Root: true, AccessMode: AccessRO, CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}},
	} {
		if err := controller.SetDesired(entry); err == nil {
			t.Fatalf("malformed desired root accepted: %+v", entry)
		}
	}
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	if err := controller.SetDesired(Entry{Path: "/other", Root: true, AccessMode: AccessRO}); err == nil {
		t.Fatal("conflicting desired root accepted")
	}
}

func TestRootControllerRemovalCancelsDesiredRetry(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT)},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	if err := controller.SetDesired(root); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)
	writer.nextAttempt(t)
	timer := timers.next(t, 10*time.Millisecond)
	if err := controller.RemoveDesired(root.Path); err != nil {
		t.Fatal(err)
	}
	select {
	case <-timer.stopped:
	case <-t.Context().Done():
		t.Fatal("removed root left retry timer active")
	}
	if got := len(writer.attempts); got != 0 {
		t.Fatalf("removed root retried %d times", got)
	}
	select {
	case extra := <-timers.created:
		t.Fatalf("removed root scheduled another timer: %v", extra.duration)
	default:
	}
}

func TestRootControllerRemovalThenRecreateConverges(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT), nil},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	if err := controller.SetDesired(root); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)
	writer.nextAttempt(t)
	timer := timers.next(t, 10*time.Millisecond)

	if err := controller.RemoveDesired(root.Path); err != nil {
		t.Fatal(err)
	}
	select {
	case <-timer.stopped:
	case <-t.Context().Done():
		t.Fatal("removed root left retry timer active")
	}
	if err := controller.SetDesired(root); err != nil {
		t.Fatal(err)
	}
	writer.nextAttempt(t)
	timers.next(t, time.Minute)
}

func TestRootControllerRemovalRecreateDoesNotLoseNewDesiredWakeup(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT), nil},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	if err := controller.SetDesired(root); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)
	writer.nextAttempt(t)
	timer := timers.next(t, 10*time.Millisecond)

	recreated := make(chan error, 1)
	timer.stopHook = func() { recreated <- controller.SetDesired(root) }
	if err := controller.RemoveDesired(root.Path); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-recreated:
		if err != nil {
			t.Fatal(err)
		}
	case <-t.Context().Done():
		t.Fatal("retry timer was not stopped")
	}
	writer.nextAttempt(t)
	timers.next(t, time.Minute)
}

func TestServerAuthorizedAuthAnswerWakesRootController(t *testing.T) {
	timers := newManualRootTimers()
	writer := &scriptedRootWriter{
		outcomes: []error{rootBodyError(unix.ENOENT), nil},
		attempts: make(chan Entry, 2),
	}
	controller := newTestRootController(writer, timers)
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	volume := Entry{Path: "/tank/a", UUID: [16]byte{1}, CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}}
	server := NewServer(NewMemTable(root, volume), nil)
	server.SetRootController(controller)
	if err := controller.SetDesired(root); err != nil {
		t.Fatal(err)
	}
	startRootController(t, controller)
	writer.nextAttempt(t)
	timers.next(t, 10*time.Millisecond)

	var answers []string
	if err := server.AnswerRequest(ChannelAuthUnixIP, "nfsd 10.1.2.3\n", func(answer string) error {
		answers = append(answers, answer)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	writer.nextAttempt(t)
	if len(answers) != 1 || !strings.HasSuffix(answers[0], " *\n") {
		t.Fatalf("auth answers = %q", answers)
	}
}

func TestServerAuthWriteFailureDoesNotWakeRootController(t *testing.T) {
	controller := newRootController(&scriptedRootWriter{attempts: make(chan Entry, 1)}, nil)
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	volume := Entry{Path: "/tank/a", CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}}
	server := NewServer(NewMemTable(root, volume), nil)
	server.SetRootController(controller)

	if err := server.AnswerRequest(ChannelAuthUnixIP, "nfsd 10.1.2.3\n", func(string) error {
		return unix.EIO
	}); !errors.Is(err, unix.EIO) {
		t.Fatalf("auth write error = %v, want EIO", err)
	}
	if got := len(controller.kick); got != 0 {
		t.Fatalf("failed auth write queued %d root wakeups", got)
	}
}

func TestFirstClientResponderOrderingAuthExportFH(t *testing.T) {
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	volume := Entry{Path: "/tank/a", UUID: [16]byte{1}, CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}}
	server := NewServer(NewMemTable(root, volume), nil)

	var events []Channel
	for _, request := range []struct {
		channel Channel
		line    string
	}{
		{ChannelAuthUnixIP, "nfsd 10.1.2.3\n"},
		{ChannelExport, "* /tank\n"},
		{ChannelExpKey, "* 0 \\x00000000\n"},
	} {
		if err := server.AnswerRequest(request.channel, request.line, func(string) error {
			events = append(events, request.channel)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if want := []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestServerUnauthorizedAuthAnswerDoesNotWakeRootController(t *testing.T) {
	controller := newRootController(&scriptedRootWriter{attempts: make(chan Entry, 1)}, nil)
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	volume := Entry{Path: "/tank/a", CIDRs: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")}}
	server := NewServer(NewMemTable(root, volume), nil)
	server.SetRootController(controller)
	var answer string
	if err := server.AnswerRequest(ChannelAuthUnixIP, "nfsd 192.0.2.1\n", func(line string) error {
		answer = line
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fields := strings.Fields(answer); len(fields) != 3 {
		t.Fatalf("unauthorized answer = %q", answer)
	}
	if got := len(controller.kick); got != 0 {
		t.Fatalf("unauthorized answer queued %d root wakeups", got)
	}
}

func TestRootControllerOnlyWritesPositiveExportCache(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	var channels []Channel
	writeCache = func(channel Channel, _ string) error {
		channels = append(channels, channel)
		return nil
	}
	if err := NewChannelWriter(nil).InstallRootPositive(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(channels, []Channel{ChannelExport}) {
		t.Fatalf("positive cache channels = %v, want export only", channels)
	}
}

func TestRetryableRootInstallErrorClassification(t *testing.T) {
	for _, errno := range []error{unix.ENOENT, unix.EAGAIN, unix.ETIMEDOUT, unix.EIO, unix.ENODEV, unix.ENOMEM} {
		if !isRetryableRootInstallError(rootBodyError(errno)) {
			t.Errorf("%v classified terminal", errno)
		}
	}
	for _, errno := range []error{unix.EINVAL, unix.EACCES, unix.EPERM, errors.New("malformed desired state")} {
		if isRetryableRootInstallError(rootBodyError(errno)) {
			t.Errorf("%v classified retryable", errno)
		}
	}
}

func TestRootControllerDefaultBackoffUsesJitterWithinConfiguredCap(t *testing.T) {
	controller := NewRootController(&scriptedRootWriter{}, nil)
	controller.retryMax = time.Nanosecond
	if got := controller.jitter(2 * time.Nanosecond); got != controller.retryMax {
		t.Fatalf("jittered delay = %s, want configured cap %s", got, controller.retryMax)
	}

	controller.retryMax = defaultRootRetryMax
	seen := make(map[time.Duration]struct{})
	for range 64 {
		delay := controller.jitter(controller.retryMax)
		if delay < 4*controller.retryMax/5 || delay > controller.retryMax {
			t.Fatalf("jittered delay %s outside [%s,%s]", delay, 4*controller.retryMax/5, controller.retryMax)
		}
		seen[delay] = struct{}{}
	}
	if len(seen) == 1 {
		t.Fatal("default backoff did not jitter")
	}
}

func TestRootControllerRetryTaxonomy(t *testing.T) {
	retryableErrnos := []error{
		unix.ENOENT, unix.EINTR, unix.EAGAIN, unix.ETIMEDOUT, unix.EIO,
		unix.ENODEV, unix.ENOMEM, unix.ENFILE, unix.EMFILE, unix.EBUSY,
		unix.ENOSPC,
	}
	for _, phase := range []cacheWritePhase{cacheWriteOpen, cacheWriteBody} {
		for _, errno := range retryableErrnos {
			err := &cacheWriteError{channel: ChannelExport, phase: phase, err: errno}
			if !isRetryableRootInstallError(err) {
				t.Errorf("phase=%v errno=%v: want retryable", phase, errno)
			}
		}
	}
	for _, errno := range []error{unix.EINVAL, unix.EACCES, unix.EPERM} {
		if isRetryableRootInstallError(rootBodyError(errno)) {
			t.Errorf("errno=%v: want terminal", errno)
		}
	}
	if isRetryableRootInstallError(rootOpenError(unix.EBADF)) {
		t.Fatal("EBADF outside shutdown must be terminal")
	}
	if isRetryableRootInstallError(errors.New("programmer state error")) {
		t.Fatal("unwrapped error must be terminal")
	}
}

func TestRootControllerOperationalFailureRetriesAfterPriorSuccess(t *testing.T) {
	writer := &scriptedRootWriter{outcomes: []error{nil, rootBodyError(unix.ENOSPC), nil}, attempts: make(chan Entry, 3)}
	timers := newManualRootTimers()
	controller := newTestRootController(writer, timers)
	startRootController(t, controller)
	if err := controller.SetDesired(Entry{Path: "/tank", Root: true, AccessMode: AccessRO}); err != nil {
		t.Fatal(err)
	}
	writer.nextAttempt(t)
	refresh := timers.next(t, time.Minute)
	refresh.fire()
	writer.nextAttempt(t)
	retry := timers.next(t, 10*time.Millisecond)
	retry.fire()
	writer.nextAttempt(t)
	timers.next(t, time.Minute)
}
