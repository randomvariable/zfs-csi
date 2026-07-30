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

package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// IdentityServer implements the CSI Identity service. It is stateless and served
// by all three modes (controller, storage-agent, node).
type IdentityServer struct {
	csi.UnimplementedIdentityServer

	// ready is consulted by Probe; flipped true once the mode's subsystems report healthy.
	ready func(ctx context.Context) (bool, error)
}

// NewIdentityServer returns an IdentityServer. ready is a function that reports
// whether this process is ready to serve; nil means always-ready.
func NewIdentityServer(ready func(context.Context) (bool, error)) *IdentityServer {
	if ready == nil {
		ready = func(context.Context) (bool, error) { return true, nil }
	}

	return &IdentityServer{ready: ready}
}

// GetPluginInfo returns the driver name + version.
func (s *IdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: PluginName, VendorVersion: PluginVersion}, nil
}

// GetPluginCapabilities advertises the controller service + topology constraints.
func (s *IdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{Capabilities: identityCapabilities()}, nil
}

// Probe reports readiness. Returns FAILED_PRECONDITION if not ready (sidecar livenessprobe uses this).
func (s *IdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	ok, err := s.ready(ctx)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "probe: %v", err)
	}

	return &csi.ProbeResponse{
		Ready: wrapperspb.Bool(ok),
	}, nil
}
