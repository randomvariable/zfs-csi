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
	"fmt"
	"os"
	"strings"
)

func LiveEnabled() bool { return strings.TrimSpace(os.Getenv("ZFS_CSI_PERF")) == "1" }

func FioImage() (string, error) {
	image := strings.TrimSpace(os.Getenv("ZFS_CSI_PERF_FIO_IMAGE"))
	if image == "" {
		return "", fmt.Errorf("ZFS_CSI_PERF_FIO_IMAGE must pin an fio image by digest")
	}
	if !strings.Contains(image, "@sha256:") {
		return "", fmt.Errorf("ZFS_CSI_PERF_FIO_IMAGE must use an immutable sha256 digest")
	}
	return image, nil
}
