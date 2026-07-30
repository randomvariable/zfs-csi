package nfsexport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type ChannelWriter struct {
	mu sync.Mutex
}

func NewChannelWriter(logf Logf) *ChannelWriter {
	_ = logf
	return &ChannelWriter{}
}

// InstallRootPositive applies one idempotent explicit-root correction to the
// kernel nfsd.export cache. ENOENT is expected until auth.unix.ip establishes
// domain "*"; RootController classifies and retries it. No nfsd.fh positive is
// ever written proactively.
func (w *ChannelWriter) InstallRootPositive(entry Entry) error {
	if !entry.Root {
		return errors.New("nfsexport: positive root install requires Root entry")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return writeExportCache(entry, entry.Path, expiryUnix(), false)
}

// InvalidateEntry removes stale kernel cache keys using the authoritative
// surviving table.
func (w *ChannelWriter) InvalidateEntry(entry Entry, clients []string, surviving []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var survivingPrefixes []netip.Prefix
	for _, survivorEntry := range surviving {
		if survivorEntry.Path != entry.Path {
			survivingPrefixes = append(survivingPrefixes, survivorEntry.CIDRs...)
		}
	}
	exp := expiryUnix()
	for _, path := range pathComponents(entry.Path) {
		if err := writeExportCache(Entry{}, path, exp, true); err != nil {
			// svc_export_parse returns ENOENT for every CACHE_NEGATIVE answer
			// whenever either the auth domain or the export path is absent, so
			// write-body ENOENT is the desired fail-closed result even when the
			// path still exists. Channel-open failures remain hard.
			return err
		}
		if path == "/" {
			// The host root is never the NFS root in the explicit-root model:
			// skip writing a fsid=0 negative here so we do not invalidate the
			// real /tank root by accident. Per-volume invalidation only touches
			// UUID-keyed pseudo entries; the reconciler calls InvalidateRoot
			// when the root itself is withdrawn on last-volume removal.
			continue
		}
		pseudo := Entry{Path: path, UUID: pseudoUUID(path), pseudo: true}
		if path == entry.Path {
			pseudo = entry
		}
		if err := writeCache(ChannelExpKey, marshalExpKeyAnswer("*", fsidTypeUUID16, pseudo.UUID[:], exp, "", true)); err != nil && !isNegativeParserENOENT(err, true) {
			return err
		}
	}
	clients = append([]string(nil), clients...)
	for i := range clients {
		clients[i] = strings.TrimSpace(clients[i])
	}
	sort.Strings(clients)
	for i, client := range clients {
		if i > 0 && client == clients[i-1] {
			continue
		}
		addr, err := netip.ParseAddr(client)
		if err != nil {
			return err
		}
		if coveredByPrefixes(addr, survivingPrefixes) {
			continue
		}
		if err := writeCache(ChannelAuthUnixIP, marshalIPMapAnswer("nfsd", addr, exp, "", true)); err != nil && !isNegativeParserENOENT(err, true) {
			return err
		}
	}
	return nil
}

func coveredByPrefixes(addr netip.Addr, prefixes []netip.Prefix) bool {
	addr = addr.Unmap()
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// InvalidateRoot removes the root export cache entries when the last volume
// is withdrawn. It writes a CACHE_NEGATIVE nfsd.export entry for the root path
// and a fsid=0 nfsd.fh negative so future lookups fail closed until the
// reconciler re-establishes the root.
func (w *ChannelWriter) InvalidateRoot(root string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	exp := expiryUnix()
	if err := writeExportCache(Entry{}, root, exp, true); err != nil {
		return err
	}
	return writeCache(ChannelExpKey, marshalExpKeyAnswer("*", fsidTypeNum, []byte{0, 0, 0, 0}, exp, "", true))
}

var writeCache = func(ch Channel, line string) error {
	p := filepath.Join(procRPCRoot, channelFile[ch])
	fd, err := unix.Open(p, unix.O_WRONLY, 0)
	if err != nil {
		return &cacheWriteError{channel: ch, phase: cacheWriteOpen, err: fmt.Errorf("nfsexport: open %s: %w", channelFile[ch], err)}
	}
	defer unix.Close(fd)
	_, err = unix.Write(fd, []byte(line))
	if err != nil {
		return &cacheWriteError{channel: ch, phase: cacheWriteBody, err: fmt.Errorf("nfsexport: write %s: %w", channelFile[ch], err)}
	}
	return nil
}

type cacheWritePhase uint8

const (
	cacheWriteOpen cacheWritePhase = iota
	cacheWriteBody
)

type cacheWriteError struct {
	channel Channel
	phase   cacheWritePhase
	err     error
}

func (e *cacheWriteError) Error() string { return e.err.Error() }
func (e *cacheWriteError) Unwrap() error { return e.err }

func isNegativeParserENOENT(err error, negative bool) bool {
	var writeErr *cacheWriteError
	// Each sunrpc cache parser (svc_export_parse, expkey_parse, auth_unix_parse)
	// returns ENOENT for every CACHE_NEGATIVE answer whenever the referenced
	// auth domain, export path, or expkey is absent — the desired fail-closed
	// result, not a channel-runtime failure. The phase guard excludes
	// channel-open ENOENT (channel gone); negative excludes positive writes.
	return negative && errors.As(err, &writeErr) &&
		writeErr.phase == cacheWriteBody &&
		errors.Is(err, unix.ENOENT)
}

func writeExportCache(entry Entry, path string, exp int64, negative bool) error {
	err := writeCache(ChannelExport, marshalExportAnswer(entry, "*", path, exp, negative))
	if err != nil && !isNegativeParserENOENT(err, negative) {
		return err
	}
	return nil
}

// CheckRuntimeStructure fails unless every cache channel needed by the
// responder exists. It deliberately does not require auth-domain/cache content,
// which is created reactively by the first authorized client.
func CheckRuntimeStructure() error {
	for _, ch := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
		path := filepath.Join(procRPCRoot, channelFile[ch])
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("nfsexport: stat %s: %w", channelFile[ch], err)
		}
	}
	return nil
}

func flushAll() error {
	var errs []error
	for _, ch := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
		if err := writeFlush(ch); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

var procRPCRoot = "/proc/net/rpc"
var channelFile = map[Channel]string{ChannelAuthUnixIP: "auth.unix.ip/channel", ChannelExpKey: "nfsd.fh/channel", ChannelExport: "nfsd.export/channel"}
var flushFile = map[Channel]string{ChannelAuthUnixIP: "auth.unix.ip/flush", ChannelExpKey: "nfsd.fh/flush", ChannelExport: "nfsd.export/flush"}

type Logf func(format string, args ...any)

// Server runs the upcall responder against the live kernel cache channels. It
// opens each channel file and, per channel, reads pending upcall requests and
// writes answers computed by the Responder against the current ExportTable.
//
// One goroutine services each channel. The cache channels are read with RAW
// BLOCKING syscalls (unix.Read on a fd opened outside the Go runtime's
// netpoller): the /proc/net/rpc channel files do not report readability via
// poll/epoll, so an os.File read parked in the netpoller would never wake when
// the kernel queues an upcall. A blocking read parks the goroutine in the
// kernel until a request arrives — the same mechanism rpc.mountd uses. The
// context cancels the loops by closing the fds, which unblocks the reads.
type Server struct {
	responder *Responder
	logf      Logf
	root      *RootController

	mu     sync.Mutex
	fds    []int
	closed bool
}

// NewServer builds a Server answering against the given table.
func NewServer(table ExportTable, logf Logf) *Server {
	return &Server{responder: NewResponder(table), logf: logf}
}

// SetRootController connects successful authorized auth-domain answers to the
// proactive root loop. It must be called before Run.
func (s *Server) SetRootController(controller *RootController) {
	s.root = controller
}

func (s *Server) log(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
	}
}

// Run opens the three cache channels and services them until ctx is cancelled.
// It returns the first fatal error (channel open failure); per-request decode
// errors are logged and skipped so one malformed upcall cannot stop the
// responder. Run blocks until every channel goroutine has exited.
func (s *Server) Run(ctx context.Context) error {
	channels := []Channel{ChannelAuthUnixIP, ChannelExpKey, ChannelExport}

	opened := make(map[Channel]int, len(channels))
	for _, ch := range channels {
		fd, err := openChannel(ch)
		if err != nil {
			// Close any already-opened channels before returning.
			for _, ofd := range opened {
				_ = unix.Close(ofd)
			}
			return fmt.Errorf("nfsexport: open %s: %w", channelFile[ch], err)
		}
		opened[ch] = fd
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		for _, fd := range opened {
			_ = unix.Close(fd)
		}
		return errors.New("nfsexport: server already closed")
	}
	for _, fd := range opened {
		s.fds = append(s.fds, fd)
	}
	s.mu.Unlock()

	// Close fds when ctx is done so blocked reads unblock and loops exit.
	stop := context.AfterFunc(ctx, s.closeFds)
	defer stop()

	var wg sync.WaitGroup
	for ch, fd := range opened {
		wg.Add(1)
		go func(ch Channel, fd int) {
			defer wg.Done()
			s.serve(ctx, ch, fd)
		}(ch, fd)
	}
	wg.Wait()
	s.closeFds()
	return nil
}

// serve reads requests from one channel fd with blocking syscalls and writes
// answers. It returns when ctx is cancelled or the fd is closed.
//
// The fd is opened by openChannel WITHOUT the Go runtime's netpoller (a raw
// unix.Open), so unix.Read is a true blocking syscall: it parks until the
// kernel queues an upcall (live channel) or returns 0 at EOF (a drained regular
// file in tests). Lines are accumulated across reads and split on '\n' because
// a single read may carry a partial or multiple requests.
func (s *Server) serve(ctx context.Context, ch Channel, fd int) {
	buf := make([]byte, 4096)
	var pending []byte
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := unix.Read(fd, buf)
		if n == 0 && err == nil {
			if sleepCtx(ctx, emptyBackoff) {
				return
			}
			continue
		}
		if n > 0 {
			pending = append(pending, buf[:n]...)
			// Process every complete line now buffered.
			for {
				idx := bytes.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				line := string(pending[:idx+1])
				pending = pending[idx+1:]
				if line == "\n" || line == "" {
					continue
				}
				if aerr := s.AnswerRequest(ch, line, func(answer string) error {
					_, err := unix.Write(fd, []byte(answer))
					return err
				}); aerr != nil {
					// Malformed upcall: log and drop. Do not stop the loop.
					s.log("nfsexport: decode %s request %q: %v", channelFile[ch], line, aerr)
				}
			}
			continue
		}
		// n == 0 (EOF: a drained regular file in tests) or a read error.
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, os.ErrClosed) ||
				err == unix.EBADF || err == unix.EINVAL {
				// fd closed for shutdown, or otherwise unusable: stop.
				return
			}
			if err == unix.EINTR {
				continue
			}
			s.log("nfsexport: read %s: %v", channelFile[ch], err)
			if sleepCtx(ctx, errorBackoff) {
				return
			}
			continue
		}
	}
}

// AnswerRequest computes and writes one response. Authorized auth positives
// wake root reconciliation only after their kernel cache write succeeds.
func (s *Server) AnswerRequest(ch Channel, line string, write func(string) error) error {
	answer, err := s.responder.Answer(ch, line)
	if err != nil {
		return err
	}
	if err := write(answer); err != nil {
		return err
	}
	if ch == ChannelAuthUnixIP && s.root != nil && authAnswerEstablishesWorldDomain(answer) {
		s.root.Kick()
	}
	return nil
}

func authAnswerEstablishesWorldDomain(answer string) bool {
	tokens, err := parseQwords(answer)
	return err == nil && len(tokens) == 4 && tokens[3].String() == "*"
}

func (s *Server) closeFds() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, fd := range s.fds {
		_ = unix.Close(fd)
	}
	s.fds = nil
}

// Flush purges all cached entries for the export and expkey caches, forcing
// the kernel to re-upcall (and receive current answers) on the next access.
// Call it after removing an export so a stale positive cache entry cannot keep
// a destroyed dataset reachable. It is best-effort: a flush-file write failure
// is logged and returned joined, never fatal.
func (s *Server) Flush() error {
	var errs []error
	// Flushing export + expkey is sufficient to drop export authorization;
	// auth.unix.ip is also flushed so a removed client CIDR stops resolving.
	for _, ch := range []Channel{ChannelExport, ChannelExpKey, ChannelAuthUnixIP} {
		if err := writeFlush(ch); err != nil {
			s.log("nfsexport: flush %s: %v", flushFile[ch], err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// writeFlush is a var so tests can substitute a temp-file flush. The default
// writes "0\n" to /proc/net/rpc/<name>/flush; the kernel ignores the value and
// purges everything.
var writeFlush = func(ch Channel) error {
	p := filepath.Join(procRPCRoot, flushFile[ch])
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString("0\n")
	return err
}

const (
	// emptyBackoff throttles re-reads when a channel has no pending request.
	emptyBackoff = 200 * time.Millisecond
	// errorBackoff throttles re-reads after a transient read error.
	errorBackoff = time.Second
)

// sleepCtx sleeps for d or until ctx is cancelled; it reports whether ctx was
// cancelled.
func sleepCtx(ctx context.Context, d time.Duration) (cancelled bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}

// openChannel is a var so tests can substitute a temp-file opener. The default
// opens the real /proc/net/rpc/<name>/channel with a RAW unix.Open (O_RDWR).
// Crucially this fd is NOT registered with the Go runtime netpoller (os.File
// would register it), so unix.Read in serve blocks in-kernel until the kernel
// queues an upcall — the cache channels do not support poll/epoll readiness.
var openChannel = func(ch Channel) (int, error) {
	p := filepath.Join(procRPCRoot, channelFile[ch])
	return unix.Open(p, unix.O_RDWR, 0)
}
