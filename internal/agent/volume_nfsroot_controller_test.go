package agent

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/randomvariable/zfs-csi/internal/nfsexport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type testRootIntentController struct {
	desired   []nfsexport.Entry
	removed   []string
	events    *[]string
	setErr    error
	removeErr error
	active    string
	mu        sync.Mutex
}

func (c *testRootIntentController) hasDesired(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active == path
}

func (c *testRootIntentController) SetDesired(entry nfsexport.Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events != nil {
		*c.events = append(*c.events, "set-root")
	}
	if c.setErr != nil {
		return c.setErr
	}
	c.desired = append(c.desired, entry)
	c.active = entry.Path
	return nil
}

func (c *testRootIntentController) RemoveDesired(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events != nil {
		*c.events = append(*c.events, "remove-root")
	}
	if c.removeErr != nil {
		return c.removeErr
	}
	c.removed = append(c.removed, path)
	if c.active == path {
		c.active = ""
	}
	return nil
}

func TestNFSDurableRootIntentAllowsChildRegistrationWithoutCacheConvergence(t *testing.T) {
	table := nfsexport.NewMemTable()
	controller := &testRootIntentController{}
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.NFSExports = table
	r.NFSRootController = controller
	r.RootProbe = func(context.Context, string) error { return nil }
	vol := nfsTestVolume("10.0.0.1/32")

	if err := r.registerNFSExportCtx(t.Context(), vol, "tank/a", "/tank/a"); err != nil {
		t.Fatalf("register export after durable root intent: %v", err)
	}
	if _, ok := table.LookupExport("*", "/tank/a"); !ok {
		t.Fatal("volume export not registered")
	}
	if len(controller.desired) != 1 || controller.desired[0].Path != "/tank" {
		t.Fatalf("desired roots = %+v", controller.desired)
	}
}

func TestNFSRootStructuralPreflightGatesDesiredIntent(t *testing.T) {
	table := nfsexport.NewMemTable()
	controller := &testRootIntentController{}
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.NFSExports = table
	r.NFSWriter = &fakeNFSCacheWriter{}
	r.NFSRootController = controller
	r.RootProbe = func(context.Context, string) error { return errors.New("nfsd runtime unavailable") }

	if err := r.registerNFSExportCtx(t.Context(), nfsTestVolume("10.0.0.1/32"), "tank/a", "/tank/a"); err == nil {
		t.Fatal("registration succeeded despite structural preflight failure")
	}
	if len(controller.desired) != 0 {
		t.Fatalf("desired roots = %+v", controller.desired)
	}
}

func TestNFSRootDesiredIntentFailureRollsBackTable(t *testing.T) {
	table := nfsexport.NewMemTable()
	controller := &testRootIntentController{setErr: errors.New("reject desired root")}
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.NFSExports = table
	r.NFSRootController = controller
	r.RootProbe = func(context.Context, string) error { return nil }

	if err := r.registerNFSExportCtx(t.Context(), nfsTestVolume("10.0.0.1/32"), "tank/a", "/tank/a"); err == nil {
		t.Fatal("registration succeeded despite desired-root failure")
	}
	if _, ok := table.Root(); ok {
		t.Fatal("root table survived desired-root failure")
	}
}

func TestNFSLastVolumeRemovalCancelsRootIntentBeforeInvalidation(t *testing.T) {
	table := nfsexport.NewMemTable(
		nfsexport.Entry{Path: "/tank", Root: true, AccessMode: nfsexport.AccessRO},
		nfsexport.Entry{Path: "/tank/a"},
	)
	controller := &testRootIntentController{}
	if err := controller.SetDesired(nfsexport.Entry{Path: "/tank", Root: true, AccessMode: nfsexport.AccessRO}); err != nil {
		t.Fatal(err)
	}
	cacheEvents := []string{}
	writer := &fakeNFSCacheWriter{events: &cacheEvents}
	controller.events = &cacheEvents
	controller.desired = nil
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.NFSExports = table
	r.NFSWriter = writer
	r.NFSRootController = controller
	r.rootIdentity = "/tank"
	r.nfsPaths = make(map[string]string)
	r.nfsEntries = make(map[string]nfsexport.Entry)
	r.nfsPaths["tank/a"] = "/tank/a"
	r.nfsEntries["/tank/a"] = nfsexport.Entry{Path: "/tank/a"}
	vol := nfsTestVolume("10.0.0.1/32")
	vol.DeletionTimestamp = &metav1.Time{Time: time.Now()}

	if err := r.withdrawNFSExport(t.Context(), logr.Discard(), vol, "tank/a"); err != nil {
		t.Fatal(err)
	}
	if len(controller.removed) != 1 || controller.removed[0] != "/tank" {
		t.Fatalf("removed roots = %v", controller.removed)
	}
	if writer.rootInvalidations != 1 {
		t.Fatalf("root invalidations = %d", writer.rootInvalidations)
	}
	if want := []string{"invalidate", "remove-root", "invalidate-root"}; !reflect.DeepEqual(cacheEvents, want) {
		t.Fatalf("events = %v, want %v", cacheEvents, want)
	}
}

func TestNFSRegisterCompetingWithLastWithdrawCommitsRootAndChildAtomically(t *testing.T) {
	table := nfsexport.NewMemTable()
	controller := &testRootIntentController{}
	d := newRootTestDeps(t)
	r := d.reconciler()
	r.NFSExports = table
	r.NFSRootController = controller
	r.RootProbe = greenRootProbe
	volA := nfsTestVolume("10.0.0.1/32")
	volB := nfsTestVolume("10.0.0.2/32")
	if err := r.registerNFSExportCtx(t.Context(), volA, "tank/a", "/tank/a"); err != nil {
		t.Fatal(err)
	}

	writer := &blockingNFSCacheWriter{invalidateStarted: make(chan struct{}), allowInvalidate: make(chan struct{})}
	r.NFSWriter = writer
	withdrawDone := make(chan error, 1)
	go func() { withdrawDone <- r.withdrawNFSExport(t.Context(), logr.Discard(), volA, "tank/a") }()
	<-writer.invalidateStarted

	registerEntered := make(chan struct{})
	allowRegister := make(chan struct{})
	r.registerNFSExportHook = func(string, string) {
		close(registerEntered)
		<-allowRegister
	}
	registerDone := make(chan error, 1)
	go func() { registerDone <- r.registerNFSExportCtx(t.Context(), volB, "tank/b", "/tank/b") }()
	<-registerEntered
	close(allowRegister)
	close(writer.allowInvalidate)
	if err := <-withdrawDone; err != nil {
		t.Fatal(err)
	}
	if err := <-registerDone; err != nil {
		t.Fatal(err)
	}

	if _, childPresent := table.LookupExport("*", "/tank/b"); !childPresent {
		t.Fatal("competing registration did not commit child")
	}
	root, rootPresent := table.Root()
	if !rootPresent || root != "/tank" || !controller.hasDesired(root) {
		t.Fatalf("child present with root=%q present=%v desired=%v", root, rootPresent, controller.hasDesired(root))
	}
}
