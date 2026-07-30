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

package stage

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
)

// Client is the node driver's handle to a StagePlugin sidecar.
type Client struct {
	conn  *grpc.ClientConn
	stage stagepb.StagePluginClient
	log   logr.Logger
}

// Dial connects to a StagePlugin over a node-local unix socket.
func Dial(ctx context.Context, socketPath string, log logr.Logger) (*Client, error) {
	// grpc.NewClient is non-blocking; surface connection failures lazily on the
	// first RPC via the returned error (the node driver's NodeStageVolume call).
	conn, err := grpc.NewClient(
		fmt.Sprintf("unix://%s", socketPath),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial stage plugin %s: %w", socketPath, err)
	}

	return &Client{
		conn:  conn,
		stage: stagepb.NewStagePluginClient(conn),
		log:   log,
	}, nil
}

// Stage calls NodeStage on the plugin.
func (c *Client) Stage(ctx context.Context, req *stagepb.NodeStageRequest) (*stagepb.NodeStageResponse, error) {
	return c.stage.NodeStage(ctx, req)
}

// Unstage calls NodeUnstage on the plugin.
func (c *Client) Unstage(ctx context.Context, req *stagepb.NodeUnstageRequest) (*stagepb.NodeUnstageResponse, error) {
	return c.stage.NodeUnstage(ctx, req)
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
