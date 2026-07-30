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

package agent

import (
	"context"
	"fmt"
	"strconv"

	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func verifyPoolIdentity(ctx context.Context, backend zfs.Backend, pool, expected string) error {
	want, err := strconv.ParseUint(expected, 10, 64)
	if err != nil || want == 0 || strconv.FormatUint(want, 10) != expected {
		return fmt.Errorf("invalid expected pool GUID %q", expected)
	}
	live, err := backend.PoolGUID(ctx, pool)
	if err != nil {
		return fmt.Errorf("read pool %q GUID: %w", pool, err)
	}
	if live != expected {
		return fmt.Errorf("pool %q GUID mismatch: live=%q expected=%q", pool, live, expected)
	}
	return nil
}
