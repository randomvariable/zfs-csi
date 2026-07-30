// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package impl

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	mountutils "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

func TestFormatUsesUniqueProbeDirectoriesAndCleansUp(t *testing.T) {
	root := t.TempDir()
	mounter := mountutils.NewFakeMounter(nil)
	ops := New(root, mounter, &testingexec.FakeExec{DisableScripts: true})

	const formats = 16
	errs := make(chan error, formats)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < formats; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- ops.Format(context.Background(), "/dev/zvol/tank/vol", "ext4")
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Format() error = %v", err)
		}
	}

	log := mounter.GetLog()
	if len(log) != formats*2 {
		t.Fatalf("mount log length = %d, want %d", len(log), formats*2)
	}

	probes := make(map[string]struct{}, formats)
	for _, action := range log {
		if !strings.HasPrefix(filepath.Base(action.Target), "format-probe-") {
			t.Fatalf("probe target = %q, want format-probe-*", action.Target)
		}
		if action.Action == mountutils.FakeActionMount {
			probes[action.Target] = struct{}{}
		}
	}
	if len(probes) != formats {
		t.Fatalf("unique probe directories = %d, want %d", len(probes), formats)
	}

	for probe := range probes {
		if _, err := os.Stat(probe); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("probe %q still exists or stat failed: %v", probe, err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", root, err)
	}
	if len(entries) != 0 {
		t.Fatalf("probe parent %q not empty: %v", root, entries)
	}
}

func TestMountAndBindMountDelegateToInjectedMounter(t *testing.T) {
	mounter := mountutils.NewFakeMounter(nil)
	ops := New("/root", mounter, &testingexec.FakeExec{DisableScripts: true})

	if err := ops.Mount(context.Background(), "/dev/zvol/tank/vol", "stage", "xfs", []string{"noatime"}); err != nil {
		t.Fatalf("Mount() error = %v", err)
	}

	if err := ops.BindMount(context.Background(), "stage", "pods/vol", true); err != nil {
		t.Fatalf("BindMount() error = %v", err)
	}

	got := mounter.GetLog()

	want := []mountutils.FakeAction{
		{Action: mountutils.FakeActionMount, Target: "/root/stage", Source: "/dev/zvol/tank/vol", FSType: "xfs"},
		{Action: mountutils.FakeActionMount, Target: "/root/pods/vol", Source: "/dev/zvol/tank/vol", FSType: ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mount log mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestResizeUsesMountUtilsGrowCommands(t *testing.T) {
	fakeExec := &testingexec.FakeExec{ExactOrder: true}
	fakeExec.CommandScript = []testingexec.FakeCommandAction{
		fakeCommand("blkid", []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", "/dev/vol"}, []byte("TYPE=xfs\n"), nil),
		fakeCommand("xfs_growfs", []string{"-d", "/root/stage"}, nil, nil),
	}
	ops := New("/root", mountutils.NewFakeMounter(nil), fakeExec)

	if err := ops.Resize(context.Background(), "/dev/vol", "stage", "xfs"); err != nil {
		t.Fatalf("Resize() error = %v", err)
	}

	if fakeExec.CommandCalls != 2 {
		t.Fatalf("CommandCalls = %d, want 2", fakeExec.CommandCalls)
	}
}

func fakeCommand(cmd string, args []string, output []byte, err error) testingexec.FakeCommandAction {
	return func(_ string, _ ...string) utilexec.Cmd {
		fake := &testingexec.FakeCmd{
			CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) {
				return output, nil, err
			}},
		}

		return testingexec.InitFakeCmd(fake, cmd, args...)
	}
}
