// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

// Package partition provides utilities to partition requests to a cluster.
//
// Every server in the cluster receives requests meant for any
// partition but these requests are then routed to the right partition
// where it is handled.
//
// Each server in the cluster also serves traffic from other servers
// destined for its own partition.
//
// Usage;
//
// Every server in the cluster creates a router at initialization
// time:
//
//      endpointRegistry = partition.WithEndpointRegistry(...)
//      router, err := partition.New(ctx, "ownIP:2222", handler, endpointRegistry)
//
// The endpoint registry keeps track of all the servers in the cluster
// and their local addresses for inter-server communication
//
// The handler is the servers own implementation of requests routed to
// it by other servers.
//
// When an external request comes in, the server would hash the
// request and then use the router to execute it on the right server
// in its cluster:
//
//      resp, err := router.Run(ctx, hash, request)
//
//
// Note that this call would end up being executed on another server
// in the cluster (or on the local server itself if this hash is
// mapped to the local server).
//
// The effectiveness of this strategy depends on how uniform the hash
// is and the mapping strategy.  The default mechanism to map hashes
// to specific servers in the cluster is to use a
// highest-random-weight algorithm which can be overridden using the
// WithPicker option.
//
// The inter-server communication is via RPC and this also can be
// configured to alternate mechanisms using the WithNetwork option.
//
//
// The requests and responses are expected to be byte slices. For
// stronger types, protobufs can be used to serialize structures or
// the runner package (see
// https://godoc.org/github.com/tvastar/cluster/pkg/partition/runner)
// for solution using gob-encoding and reflection.
package partition

import (
	"context"
	"io"
)

// EndpointRegistry manages the live list of endpoints in a cluster.
//
// The partition package does not cache the results so any requirement
// to cache this should be
type EndpointRegistry interface {
	RegisterEndpoint(ctx context.Context, addr string) (io.Closer, error)
	ListEndpoints(ctx context.Context, refresh bool) ([]string, error)
}

// Network implements the communication network between servers in the clsuter.
type Network interface {
	DialClient(ctx context.Context, addr string) (RunCloser, error)
	RegisterServer(ctx context.Context, addr string, handler Runner) (io.Closer, error)
}

// Runner executes a single request with the specified hash
type Runner interface {
	Run(ctx context.Context, hash uint64, input []byte) ([]byte, error)
}

// RunCloser combines Runner and io.Closer
type RunCloser interface {
	Runner
	io.Closer
}

var defaultConfig = config{
	Network: NewRPCNetwork(nil),
}

// New returns a RunCloser which targets requests to
// specific endpoints based on the hash provided to the request.
//
// This automatically adds the provided address to the cluster. The
// provided handler is used to serve requests meant for the local
// server.
//
// Defaults are used for Picker and Network but EndpointRegistry must
// be specified -- no defaults are used for it.
func New(ctx context.Context, addr string, handler Runner, opts ...Option) (RunCloser, error) {
	s := &state{config: defaultConfig, addr: addr, handler: handler}
	s.config.pickEndpoint = NewPicker()
	for _, opt := range opts {
		opt(&s.config)
	}
	return s.init(ctx)
}

type config struct {
	EndpointRegistry
	Network

	pickEndpoint func(ctx context.Context, list []string, hash uint64) string
}

// Option configures the partitioning algorithm.
type Option func(c *config)

// WithEndpointRegistry specifies how the endpoints
// discovery/registration happens.
//
// There is no defaualt endpoint registry.
//
// RedisRegistry implements a Redis-based endpoint registry.
func WithEndpointRegistry(r EndpointRegistry) Option {
	return func(c *config) {
		c.EndpointRegistry = r
	}
}

// WithPicker specifies how the pick endpoints based on the hash.
//
// The default algorithm is to use the highest random weight
// algorithm (via NewPicker())
func WithPicker(picker func(ctx context.Context, list []string, hash uint64) string) Option {
	return func(c *config) {
		c.pickEndpoint = picker
	}
}

// WithNetwork provides a network implementation for servers in the
// cluster to route requests to eqch othere.
//
// The default mechanism is to use RPC (via NewRPCNetwork)
func WithNetwork(nw Network) Option {
	return func(c *config) {
		c.Network = nw
	}
}
