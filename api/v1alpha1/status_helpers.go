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

package v1alpha1

// CurrentState returns the observed Volume lifecycle state.
func (s VolumeStatus) CurrentState() VolumeState { return s.State }

// CurrentState returns the observed Snapshot lifecycle state.
func (s SnapshotStatus) CurrentState() SnapshotState { return s.State }

// Ready returns true when the Snapshot status is explicitly ready.
func (s SnapshotStatus) Ready() bool { return s.ReadyToUse }

// SizeBytes returns the observed Snapshot size in bytes.
func (s SnapshotStatus) SizeBytes() int64 { return s.Size }

// CreatedAtUnix returns the observed Snapshot creation time as unix seconds.
func (s SnapshotStatus) CreatedAtUnix() int64 { return s.CreatedAt }
