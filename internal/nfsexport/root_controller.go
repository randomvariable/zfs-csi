package nfsexport

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	defaultRootRetryInitial = 100 * time.Millisecond
	defaultRootRetryMax     = 5 * time.Second
	defaultRootRefresh      = 15 * time.Minute
)

type rootPositiveWriter interface {
	InstallRootPositive(Entry) error
}

type rootTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type realRootTimer struct{ *time.Timer }

func (t realRootTimer) C() <-chan time.Time { return t.Timer.C }

// RootController is the single owner of proactive explicit-root cache writes.
// Desired-state changes and events coalesce through one level-triggered loop.
type RootController struct {
	writer rootPositiveWriter
	logf   Logf

	mu      sync.Mutex
	desired *Entry
	failed  bool
	kick    chan struct{}

	newTimer        func(time.Duration) rootTimer
	retryInitial    time.Duration
	retryMax        time.Duration
	refreshInterval time.Duration
	jitter          func(time.Duration) time.Duration
}

func NewRootController(writer rootPositiveWriter, logf Logf) *RootController {
	return newRootController(writer, logf)
}

func newRootController(writer rootPositiveWriter, logf Logf) *RootController {
	c := &RootController{
		writer:          writer,
		logf:            logf,
		kick:            make(chan struct{}, 1),
		newTimer:        func(delay time.Duration) rootTimer { return realRootTimer{time.NewTimer(delay)} },
		retryInitial:    defaultRootRetryInitial,
		retryMax:        defaultRootRetryMax,
		refreshInterval: defaultRootRefresh,
	}
	c.jitter = func(delay time.Duration) time.Duration {
		// Symmetric 20% jitter prevents synchronized local-agent retries.
		spread := delay / 5
		if spread == 0 {
			return min(delay, c.retryMax)
		}
		return min(delay-spread+time.Duration(rand.Int64N(int64(2*spread)+1)), c.retryMax)
	}
	return c
}

func (c *RootController) log(format string, args ...any) {
	if c.logf != nil {
		c.logf(format, args...)
	}
}

// SetDesired records durable local intent and wakes reconciliation. Conflicting
// root identities are terminal; callers must remove old intent first.
func (c *RootController) SetDesired(entry Entry) error {
	if !entry.Root {
		return errors.New("nfsexport: desired root entry must set Root")
	}
	if filepath.Clean(entry.Path) != entry.Path {
		return errors.New("nfsexport: desired root path must be clean")
	}
	if err := validateEntry("", entry); err != nil {
		return fmt.Errorf("nfsexport: invalid desired root: %w", err)
	}
	c.mu.Lock()
	if c.desired != nil && c.desired.Path != entry.Path {
		c.mu.Unlock()
		return errors.New("nfsexport: conflicting desired root identity")
	}
	c.desired = &entry
	c.failed = false
	c.mu.Unlock()
	c.Kick()
	return nil
}

// RemoveDesired withdraws root intent and wakes the loop so any active retry or
// refresh timer is stopped immediately.
func (c *RootController) RemoveDesired(path string) error {
	path = filepath.Clean(path)
	c.mu.Lock()
	if c.desired == nil {
		c.mu.Unlock()
		return nil
	}
	if c.desired.Path != path {
		c.mu.Unlock()
		return errors.New("nfsexport: desired root identity mismatch")
	}
	c.desired = nil
	c.failed = false
	c.mu.Unlock()
	c.Kick()
	return nil
}

// Kick requests an early reconcile without spawning work or blocking callers.
func (c *RootController) Kick() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// Run reconciles one root write at a time. Retry sleeps live here, never in a
// Kubernetes reconcile call. It returns only after cancellation.
func (c *RootController) Run(ctx context.Context) {
	delay := c.retryInitial
	// SetDesired may precede lifecycle start; desired state itself triggers the
	// first pass, so consume its coalesced startup notification.
	select {
	case <-c.kick:
	default:
	}
	for {
		c.mu.Lock()
		if c.desired == nil || c.failed {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-c.kick:
				continue
			}
		}

		entry := *c.desired
		err := c.writer.InstallRootPositive(entry)
		next := c.refreshInterval
		if err != nil {
			if !isRetryableRootInstallError(err) {
				c.failed = true
				c.mu.Unlock()
				c.log("nfsexport: terminal root reconciliation failure for %s: %v", entry.Path, err)
				continue
			}
			next = c.jitter(delay)
			delay = min(delay*2, c.retryMax)
			c.log("nfsexport: retry root reconciliation for %s: %v", entry.Path, err)
		} else {
			delay = c.retryInitial
		}
		c.mu.Unlock()

		timer := c.newTimer(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-c.kick:
			timer.Stop()
		case <-timer.C():
		}
	}
}

func isRetryableRootInstallError(err error) bool {
	var writeErr *cacheWriteError
	if !errors.As(err, &writeErr) || writeErr.channel != ChannelExport {
		return false
	}
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.EBADF) {
		return false
	}
	var errno unix.Errno
	if !errors.As(err, &errno) {
		return false
	}
	// Cache-channel failures are operational by default. New kernel errnos must
	// converge through bounded retry rather than strand previously-green state.
	return true
}
