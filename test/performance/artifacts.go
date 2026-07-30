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

package performance

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Artifacts struct{ Root string }

func (a Artifacts) Init() error {
	for _, dir := range []string{"fio", "events"} {
		if err := os.MkdirAll(filepath.Join(a.Root, dir), 0o750); err != nil {
			return err
		}
	}
	return nil
}

func (a Artifacts) WriteRun(run Run) error { return writeJSON(filepath.Join(a.Root, "run.json"), run) }

func (a Artifacts) AppendSample(sample Sample) error {
	return appendJSONL(filepath.Join(a.Root, "samples.jsonl"), sample)
}

func (a Artifacts) AppendEvent(event Event) error {
	return appendJSONL(filepath.Join(a.Root, "events", "lifecycle.jsonl"), event)
}

func (a Artifacts) WriteFio(name string, raw []byte) (string, error) {
	var check any
	if err := json.Unmarshal(raw, &check); err != nil {
		return "", fmt.Errorf("invalid fio JSON: %w", err)
	}
	path := filepath.Join(a.Root, "fio", name+".json")
	return path, os.WriteFile(path, raw, 0o640)
}

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o640)
}

func appendJSONL(path string, value any) (retErr error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := bufio.NewWriter(f)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return err
	}
	return w.Flush()
}
