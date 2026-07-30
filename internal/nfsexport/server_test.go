package nfsexport

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestChannelWriterRegisterPathHasNoPositiveWriteAPI(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	var writes int
	writeCache = func(Channel, string) error { writes++; return nil }
	_ = NewChannelWriter(nil)
	if writes != 0 {
		t.Fatalf("cache writes = %d, want none", writes)
	}
}

func TestWriteCacheOpenFailureIdentifiesChannel(t *testing.T) {
	oldRoot := procRPCRoot
	t.Cleanup(func() { procRPCRoot = oldRoot })
	procRPCRoot = t.TempDir()
	for _, ch := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
		ch := ch
		t.Run(channelFile[ch], func(t *testing.T) {
			path := filepath.Join(procRPCRoot, channelFile[ch])
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			err := writeCache(ch, "ignored")
			if err == nil || !strings.Contains(err.Error(), "nfsexport: open "+channelFile[ch]) || !errors.Is(err, unix.EISDIR) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestWriteCacheWriteFailureIdentifiesChannel(t *testing.T) {
	oldRoot := procRPCRoot
	t.Cleanup(func() { procRPCRoot = oldRoot })
	procRPCRoot = t.TempDir()
	for _, ch := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
		ch := ch
		t.Run(channelFile[ch], func(t *testing.T) {
			path := filepath.Join(procRPCRoot, channelFile[ch])
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/dev/full", path); err != nil {
				t.Fatal(err)
			}
			err := writeCache(ch, "ignored")
			if err == nil || !strings.Contains(err.Error(), "nfsexport: write "+channelFile[ch]) || !errors.Is(err, unix.ENOSPC) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestChannelWriterInvalidateToleratesMissingNegativeExportAndContinues(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	var ops []string
	writeCache = func(ch Channel, line string) error {
		ops = append(ops, channelFile[ch]+":"+line)
		if ch == ChannelExport && strings.Contains(line, "gone") {
			return &cacheWriteError{channel: ch, phase: cacheWriteBody, err: unix.ENOENT}
		}
		return nil
	}
	entry := Entry{Path: filepath.Join(t.TempDir(), "gone"), UUID: [16]byte{1}}
	if err := NewChannelWriter(nil).InvalidateEntry(entry, []string{"10.0.0.1"}, nil); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ops, "")
	if !strings.Contains(joined, channelFile[ChannelExpKey]) || !strings.Contains(joined, channelFile[ChannelAuthUnixIP]) || !strings.Contains(joined, fmt.Sprintf("\\x%x", entry.UUID)) {
		t.Fatalf("invalidation operations = %v", ops)
	}
}

func TestChannelWriterInvalidateSkipsClientCoveredBySurvivor(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	var ops []string
	writeCache = func(ch Channel, line string) error { ops = append(ops, channelFile[ch]+":"+line); return nil }
	addr := netip.MustParseAddr("10.1.2.3")
	removed := Entry{Path: "/tank/removed", UUID: [16]byte{1}}
	survivor := Entry{Path: "/tank/survivor", CIDRs: []netip.Prefix{mustPrefix(t, "10.1.2.0/24")}}
	if err := NewChannelWriter(nil).InvalidateEntry(removed, []string{addr.String(), "192.0.2.1"}, []Entry{survivor}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ops, "")
	if strings.Contains(joined, " 10.1.2.3 ") || !strings.Contains(joined, " 192.0.2.1 ") {
		t.Fatalf("auth negatives = %v", ops)
	}
}

func TestChannelWriterInvalidateDoesNotMutateClients(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	writeCache = func(Channel, string) error { return nil }
	clients := []string{" 10.0.0.2 ", "10.0.0.1"}
	if err := NewChannelWriter(nil).InvalidateEntry(Entry{Path: "/tank/a"}, clients, nil); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(clients, ","); got != " 10.0.0.2 ,10.0.0.1" {
		t.Fatalf("clients mutated = %q", got)
	}
}

func TestNegativeParserENOENTExceptionScope(t *testing.T) {
	for _, ch := range []Channel{ChannelExport, ChannelExpKey, ChannelAuthUnixIP} {
		bodyENOENT := &cacheWriteError{channel: ch, phase: cacheWriteBody, err: unix.ENOENT}
		if !isNegativeParserENOENT(bodyENOENT, true) {
			t.Fatalf("negative %s body ENOENT not tolerated", channelFile[ch])
		}
		if isNegativeParserENOENT(bodyENOENT, false) {
			t.Fatalf("positive %s ENOENT was tolerated", channelFile[ch])
		}
		openENOENT := &cacheWriteError{channel: ch, phase: cacheWriteOpen, err: unix.ENOENT}
		if isNegativeParserENOENT(openENOENT, true) {
			t.Fatalf("%s channel-open ENOENT was tolerated", channelFile[ch])
		}
	}
	nonENOENT := &cacheWriteError{channel: ChannelExport, phase: cacheWriteBody, err: unix.EINVAL}
	if isNegativeParserENOENT(nonENOENT, true) {
		t.Fatal("non-ENOENT write error was tolerated")
	}
}

func TestChannelWriterInvalidateToleratesAncestorNegativeExportENOENT(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	existingAncestor := t.TempDir()
	var ops []string
	writeCache = func(ch Channel, line string) error {
		ops = append(ops, channelFile[ch]+":"+line)
		if ch == ChannelExport && strings.Contains(line, existingAncestor) {
			return &cacheWriteError{channel: ch, phase: cacheWriteBody, err: unix.ENOENT}
		}
		return nil
	}
	entry := Entry{Path: filepath.Join(existingAncestor, "gone"), UUID: [16]byte{1}}
	if err := NewChannelWriter(nil).InvalidateEntry(entry, []string{"10.0.0.1"}, nil); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ops, "")
	if !strings.Contains(joined, channelFile[ChannelExpKey]) || !strings.Contains(joined, channelFile[ChannelAuthUnixIP]) {
		t.Fatalf("invalidation stopped early: %v", ops)
	}
}

func TestChannelWriterInvalidateToleratesNegativeParserENOENTOnAllChannels(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	var ops []string
	writeCache = func(ch Channel, line string) error {
		ops = append(ops, channelFile[ch])
		return &cacheWriteError{channel: ch, phase: cacheWriteBody, err: unix.ENOENT}
	}
	entry := Entry{Path: "/tank/gone", UUID: [16]byte{1}}
	if err := NewChannelWriter(nil).InvalidateEntry(entry, []string{"10.0.0.1"}, nil); err != nil {
		t.Fatalf("InvalidateEntry with all-channel parser ENOENT: %v", err)
	}
	joined := strings.Join(ops, ",")
	for _, ch := range []Channel{ChannelExport, ChannelExpKey, ChannelAuthUnixIP} {
		if !strings.Contains(joined, channelFile[ch]) {
			t.Fatalf("%s never attempted: %s", channelFile[ch], joined)
		}
	}
}

func TestPositiveExportENOENTPropagates(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	writeCache = func(Channel, string) error {
		return &cacheWriteError{channel: ChannelExport, phase: cacheWriteBody, err: unix.ENOENT}
	}
	err := writeExportCache(Entry{Path: "/gone"}, filepath.Join(t.TempDir(), "gone"), 1, false)
	if !errors.Is(err, unix.ENOENT) {
		t.Fatalf("positive export error = %v, want ENOENT", err)
	}
}

func TestChannelWriterInstallRootPositiveWritesExport(t *testing.T) {
	old := writeCache
	t.Cleanup(func() { writeCache = old })
	var gotChannel Channel
	var gotLine string
	writeCache = func(ch Channel, line string) error {
		gotChannel, gotLine = ch, line
		return nil
	}
	root := Entry{Path: "/tank", Root: true, AccessMode: AccessRO}
	if err := NewChannelWriter(nil).InstallRootPositive(root); err != nil {
		t.Fatal(err)
	}
	if gotChannel != ChannelExport || !strings.Contains(gotLine, "* /tank ") {
		t.Fatalf("write = channel %v line %q", gotChannel, gotLine)
	}
	if !strings.Contains(gotLine, fmt.Sprintf(" %d 65534 65534 0 ", root.exportFlags())) {
		t.Fatalf("root positive missing flags/fsid: %q", gotLine)
	}
}

func TestCheckRuntimeStructureRequiresAllChannelsWithoutCacheContent(t *testing.T) {
	t.Run("empty channels suffice", func(t *testing.T) {
		oldRoot := procRPCRoot
		t.Cleanup(func() { procRPCRoot = oldRoot })
		procRPCRoot = t.TempDir()
		for _, ch := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
			path := filepath.Join(procRPCRoot, channelFile[ch])
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := CheckRuntimeStructure(); err != nil {
			t.Fatal(err)
		}
	})
	for _, missing := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
		t.Run(channelFile[missing], func(t *testing.T) {
			oldRoot := procRPCRoot
			t.Cleanup(func() { procRPCRoot = oldRoot })
			procRPCRoot = t.TempDir()
			for _, ch := range []Channel{ChannelAuthUnixIP, ChannelExport, ChannelExpKey} {
				if ch == missing {
					continue
				}
				path := filepath.Join(procRPCRoot, channelFile[ch])
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := CheckRuntimeStructure(); err == nil || !strings.Contains(err.Error(), channelFile[missing]) {
				t.Fatalf("missing channel error = %v", err)
			}
		})
	}
}
