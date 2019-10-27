// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition

import (
	"context"
	"io"

	"github.com/tvastar/cluster/pkg/partition/internal/rpc"
	"google.golang.org/grpc"
)

// NewRPCNetwork creates a new network setup.
//
// If a grpc.Server is not provided, one is automatically created
func NewRPCNetwork(server *grpc.Server) Network {
	return &network{server}
}

type network struct {
	*grpc.Server
}

func (nw network) DialClient(ctx context.Context, addr string) (RunCloser, error) {
	return rpc.DialClient(ctx, addr)
}

func (nw network) RegisterServer(ctx context.Context, addr string, handler Runner) (io.Closer, error) {
	return rpc.RegisterServer(ctx, nw.Server, addr, handler)
}
