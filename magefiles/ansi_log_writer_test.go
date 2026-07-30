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

//go:build mage

package main

import (
	"bytes"
	"testing"
)

func TestANSILogWriterStripsCSISequences(t *testing.T) {
	var log bytes.Buffer
	w := &ansiLogWriter{dst: &log}

	input := "before \x1b[31mred\x1b[0m after \x1b[2Kdone\n"
	if _, err := w.Write([]byte(input)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got, want := log.String(), "before red after done\n"; got != want {
		t.Fatalf("log = %q, want %q", got, want)
	}
}

func TestANSILogWriterStripsSplitCSISequences(t *testing.T) {
	var log bytes.Buffer
	w := &ansiLogWriter{dst: &log}

	for _, chunk := range []string{"prefix \x1b[", "38;5;196mred", "\x1b[0m suffix"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q): %v", chunk, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got, want := log.String(), "prefix red suffix"; got != want {
		t.Fatalf("log = %q, want %q", got, want)
	}
}

func TestANSILogWriterPreservesUnicodeAndIncompleteText(t *testing.T) {
	var log bytes.Buffer
	w := &ansiLogWriter{dst: &log}

	for _, chunk := range []string{"snowman ☃ ", "\x1bX", " tail \x1b["} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q): %v", chunk, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got, want := log.String(), "snowman ☃ \x1bX tail \x1b["; got != want {
		t.Fatalf("log = %q, want %q", got, want)
	}
}
