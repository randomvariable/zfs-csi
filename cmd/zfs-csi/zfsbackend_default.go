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

// Default build (no `libzfs` tag): fail closed instead of wiring the in-memory
// fake. Production and E2E storage images MUST be built with -tags libzfs.

//go:build !libzfs && !fake_zfs_backend

package main

import "github.com/randomvariable/zfs-csi/internal/zfs"

// zfsBackend refuses to start storage paths without the real libzfs backend.
func zfsBackend() zfs.Backend {
	panic("zfs backend unavailable: rebuild with -tags libzfs for real storage, or fake_zfs_backend for tests only")
}
