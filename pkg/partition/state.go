// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition

import (
	"context"
	"io"
	"sync"
)

type state struct {
	config
	addr    string
	handler Runner

	serverCloser, epCloser io.Closer

	sync.Mutex
	clients map[string]RunCloser
}

func (s *state) Run(ctx context.Context, hash uint64, input []byte) ([]byte, error) {
	addr, err := s.getAddr(ctx, hash, false)
	if err != nil {
		return nil, err
	}

	s.Lock()
	if _, ok := s.clients[addr]; !ok && s.clients != nil {
		s.clients[addr], err = s.DialClient(ctx, addr)
	}
	c := s.clients[addr]
	s.Unlock()

	if err != nil {
		return nil, err
	}
	return c.Run(ctx, hash, input)
}

func (s *state) Close() error {
	s.Lock()
	defer s.Unlock()

	errs := errors{}
	for _, runcloser := range s.clients {
		errs.check(runcloser.Close())
	}
	s.clients = nil

	if s.epCloser != nil {
		errs.check(s.epCloser.Close())
	}
	s.epCloser = nil

	if s.serverCloser != nil {
		errs.check(s.serverCloser.Close())
	}
	s.serverCloser = nil
	return errs.toError()
}

func (s *state) init(ctx context.Context) (RunCloser, error) {
	var err error
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	if s.handler != nil {
		handler := safe{s}
		s.serverCloser, err = s.RegisterServer(ctx, s.addr, handler)
		if err != nil {
			return nil, err
		}

		s.epCloser, err = s.RegisterEndpoint(ctx, s.addr)
		if err != nil {
			return nil, err
		}
	}
	s.clients = map[string]RunCloser{}
	return s, nil
}

func (s *state) getAddr(ctx context.Context, hash uint64, refresh bool) (string, error) {
	eps, err := s.ListEndpoints(ctx, false)
	if err != nil {
		return "", err
	}

	return s.pickEndpoint(ctx, eps, hash), nil
}

// safe implements a Runner the first verifies if the request has the
// right partition
type safe struct {
	*state
}

func (s safe) Run(ctx context.Context, hash uint64, input []byte) ([]byte, error) {
	addr, err := s.getAddr(ctx, hash, false)
	if err != nil {
		return nil, err
	}
	if addr != s.addr {
		addr, err = s.getAddr(ctx, hash, true)
		if err != nil || addr != s.addr {
			return nil, IncorrectPartitionError{}
		}
	}

	return s.handler.Run(ctx, hash, input)
}
