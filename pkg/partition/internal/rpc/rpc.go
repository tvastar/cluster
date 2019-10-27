// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

// Package rpc implements partition.Network using rpc
package rpc

import (
	context "context"
	"io"
	"net"

	grpc "google.golang.org/grpc"
)

//go:generate protoc -I . ./api.proto --go_out=plugins=grpc:.

// Runner executes a single request with the specified hash
type Runner interface {
	Run(ctx context.Context, hash uint64, input []byte) ([]byte, error)
}

// RunCloser combines Runner and io.Closer
type RunCloser interface {
	Run(ctx context.Context, hash uint64, input []byte) ([]byte, error)
	Close() error
	//io.Closer
}

// DialClient creates a RPC client
func DialClient(ctx context.Context, addr string) (client, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		return client{}, err
	}
	return client{conn}, nil
}

// RegisterServer registers an RPC handler
func RegisterServer(ctx context.Context, srv *grpc.Server, addr string, handler Runner) (io.Closer, error) {
	var listener net.Listener
	if srv == nil {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		srv, listener = grpc.NewServer(), l
	}
	result := &server{srv, listener, handler}
	RegisterRunnerServer(srv, result)

	if listener != nil {
		go func() {
			if err := srv.Serve(listener); err != nil {
				// TODO: better error reporting than panic
				panic(err)
			}
		}()
	}
	return result, nil
}

type client struct {
	*grpc.ClientConn
}

func (c client) Run(ctx context.Context, hash uint64, input []byte) ([]byte, error) {
	rc := runnerClient{c.ClientConn}
	reply, err := rc.Run(ctx, &RunRequest{Input: input, Hash: hash})
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, remoteError(reply.Error)
	}
	return reply.Response, nil
}

type server struct {
	*grpc.Server
	listener net.Listener
	Runner
}

func (s *server) Run(ctx context.Context, in *RunRequest) (*RunReply, error) {
	response, err := s.Runner.Run(ctx, in.Hash, in.Input)
	if err != nil {
		return &RunReply{Response: response, Error: err.Error()}, nil
	}
	return &RunReply{Response: response}, nil
}

func (s *server) Close() error {
	if s.listener != nil {
		s.Server.GracefulStop()
		return s.listener.Close()
	}
	return nil
}

type remoteError string

func (e remoteError) Error() string {
	return string(e)
}
