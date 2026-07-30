package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/go-logr/logr"
)

const (
	nfsdPath     = "/proc/fs/nfsd"
	nfsdThreads  = 8
	nfsdGrace    = 90
	nfsdLease    = 90
	nfsdPortlist = "tcp 2049"
	nfsdVersions = "-2 -3 +4 +4.1 +4.2"
)

type nfsdProcFSOps struct {
	ReadFile  func(string) ([]byte, error)
	WriteFile func(string, []byte, os.FileMode) error
	Mount     func(string, string, string, uintptr, string) error
	MkdirAll  func(string, os.FileMode) error
}

var nfsdProcFS = nfsdProcFSOps{
	ReadFile:  os.ReadFile,
	WriteFile: os.WriteFile,
	Mount:     syscall.Mount,
	MkdirAll:  os.MkdirAll,
}

type nfsdLifecycle struct {
	owned bool
	once  sync.Once
	err   error
}

func startNFSDLifecycle(log logr.Logger) (*nfsdLifecycle, error) {
	if err := mountNFSDProcFS(); err != nil {
		return nil, err
	}

	threads, err := readNFSDThreads()
	if err != nil {
		return nil, fmt.Errorf("read nfsd threads: %w", err)
	}
	if threads != 0 {
		return nil, fmt.Errorf("nfsd already owns %d threads; refusing host-global collision", threads)
	}
	if err := writeNFSD("versions", nfsdVersions); err != nil {
		return nil, fmt.Errorf("configure nfsd versions: %w", err)
	}
	for _, setting := range []struct {
		name    string
		desired int
	}{
		{"nfsv4gracetime", nfsdGrace},
		{"nfsv4leasetime", nfsdLease},
	} {
		if err := configureNFSDOptional(log, setting.name, setting.desired); err != nil {
			return nil, fmt.Errorf("configure nfsd %s: %w", setting.name, err)
		}
	}
	if err := writeNFSD("portlist", nfsdPortlist); err != nil {
		return nil, fmt.Errorf("configure nfsd portlist: %w", err)
	}
	portlist, err := nfsdProcFS.ReadFile(filepath.Join(nfsdPath, "portlist"))
	if err != nil {
		return nil, fmt.Errorf("confirm nfsd portlist listener: %w", err)
	}
	foundListener := false
	for _, line := range strings.Split(string(portlist), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "tcp" && fields[1] == "2049" {
			foundListener = true
			break
		}
	}
	if !foundListener {
		return nil, fmt.Errorf("confirm nfsd portlist listener: tcp 2049 missing from %q", strings.TrimSpace(string(portlist)))
	}
	if err := writeNFSD("threads", strconv.Itoa(nfsdThreads)); err != nil {
		return nil, fmt.Errorf("start nfsd threads: %w", err)
	}
	threads, err = readNFSDThreads()
	if err != nil {
		return nil, fmt.Errorf("confirm nfsd threads > 0: %w", err)
	}
	if threads <= 0 {
		return nil, fmt.Errorf("confirm nfsd threads > 0: got %d", threads)
	}

	return &nfsdLifecycle{owned: true}, nil
}

func (l *nfsdLifecycle) stop() error {
	if l == nil || !l.owned {
		return nil
	}
	l.once.Do(func() { l.err = writeNFSD("threads", "0") })
	return l.err
}

func mountNFSDProcFS() error {
	_, err := nfsdProcFS.ReadFile(filepath.Join(nfsdPath, "versions"))
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("probe nfsd procfs versions: %w", err)
	}
	if err := nfsdProcFS.MkdirAll(nfsdPath, 0o555); err != nil {
		return err
	}
	return nfsdProcFS.Mount("nfsd", nfsdPath, "nfsd", 0, "")
}

func readNFSDThreads() (int, error) {
	data, err := nfsdProcFS.ReadFile(filepath.Join(nfsdPath, "threads"))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

func writeNFSD(name, value string) error {
	return nfsdProcFS.WriteFile(filepath.Join(nfsdPath, name), []byte(value+"\n"), 0)
}

func configureNFSDOptional(log logr.Logger, name string, desired int) error {
	path := filepath.Join(nfsdPath, name)
	data, err := nfsdProcFS.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		log.Info("nfsd optional control unsupported", "control", name)
		return nil
	}
	if err != nil {
		return err
	}
	current, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf("parse current value %q: %w", strings.TrimSpace(string(data)), err)
	}
	if current == desired {
		return nil
	}
	if err := writeNFSD(name, strconv.Itoa(desired)); err != nil {
		if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EBUSY) {
			log.Error(err, "nfsd optional control write ignored", "control", name, "current", current, "desired", desired)
			return nil
		}
		return err
	}
	return nil
}
