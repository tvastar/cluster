// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

// Package runner implements a multiplexer on top of the partition package.
//
// This allows the requests, responses to be more strongly typed than
// raw byte slices. The serialization protocol used is gob-encoding.
//
// This package makes heavy use of reflection.
//
// See the example for sample usage.
package runner

import (
	"bytes"
	"context"
	"encoding/gob"
	"reflect"
	"strings"

	"github.com/tvastar/cluster/pkg/partition"
)

// Handlers allows multiplexing multiple handlers with the same
// partitioner.
//
// Handlers must have the type func(Context, Req) (Res, error)
//
// Further, the Req type must implement a method `Hash() uint64`.
//
// This uses gob-encoding and so the Req/Res types must be registered
// via `gob.Register(sampleReqValue)`
//
// See example for usage details
type Handlers struct {
	part partition.RunCloser
	fns  map[string]reflect.Value
}

// Register registers function as a handler.  It returns a function
// (of the same signature as the input function).
//
// The fn must have the type func(Context, Req) (Res, error)
//
// Further, the Req type must implement a method `Hash() uint64`.
//
// The function returned can be used to make partitioned calls
// (i.e. the calls are automatically routed to the right endpoint).
func (h *Handlers) Register(fn interface{}) interface{} {
	if h.fns == nil {
		h.fns = map[string]reflect.Value{}
	}

	v := reflect.ValueOf(fn)
	t := v.Type()
	if _, ok := h.fns[t.String()]; ok {
		panic("duplicate function registration")
	}
	// TODO: t.String() is not guaranteed to be unique yo
	h.fns[t.String()] = v
	return h.wrap(t)
}

// Start creates a new partitioner under the covers and registers all
// the handlers with it.  All the functions returned by the Register
// calls can now be used.
func (h *Handlers) Start(ctx context.Context, address string, opts ...partition.Option) error {
	var err error
	h.part, err = partition.New(ctx, address, runner(h.run), opts...)
	return err
}

// Stop closes the partitioner
func (h *Handlers) Stop() error {
	return h.part.Close()
}

func (h *Handlers) run(ctx context.Context, hash uint64, input []byte) ([]byte, error) {
	s := string(input)
	idx := strings.Index(s, "|")
	return h.dispatch(ctx, h.fns[s[:idx]], []byte(s[idx+1:]))
}

func (h *Handlers) dispatch(ctx context.Context, fn reflect.Value, input []byte) ([]byte, error) {
	t := fn.Type()
	arg := reflect.New(t.In(1))
	err := gob.NewDecoder(bytes.NewReader(input)).Decode(arg.Interface())
	if err != nil {
		return nil, err
	}

	results := fn.Call([]reflect.Value{reflect.ValueOf(ctx), arg.Elem()})

	if err, _ = results[1].Interface().(error); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(results[0].Interface())
	return buf.Bytes(), err
}

func (h *Handlers) wrap(t reflect.Type) interface{} {
	if t.Kind() != reflect.Func {
		panic("not a function")
	}
	if t.NumIn() != 2 || t.NumOut() != 2 {
		panic("fn not of form func(context.Context, Request) (Response, error)")
	}
	if t.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		panic("fn 1st arg is not context.Context type")
	}
	if !t.In(1).Implements(reflect.TypeOf((*hasher)(nil)).Elem()) {
		panic("fn 2nd arg does not implement Hash() uint64")
	}
	if t.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		panic("fn 2nd result must be error")
	}

	return reflect.MakeFunc(t, func(args []reflect.Value) (results []reflect.Value) {
		var buf bytes.Buffer
		var resp []byte

		result := reflect.New(t.Out(0))
		results = []reflect.Value{reflect.Zero(t.Out(0)), reflect.Zero(t.Out(1))}

		buf.WriteString(t.String() + "|")
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(args[1].Interface())
		if err == nil {
			hash := args[1].Interface().(hasher).Hash()
			ctx := args[0].Interface().(context.Context)
			resp, err = h.part.Run(ctx, hash, buf.Bytes())
		}
		if err == nil {
			err = gob.NewDecoder(bytes.NewReader(resp)).Decode(result.Interface())
		}

		if err != nil {
			results[1] = reflect.ValueOf(err)
		} else {
			results[0] = result.Elem()
		}
		return results
	}).Interface()
}

type runner func(ctx context.Context, hash uint64, input []byte) ([]byte, error)

func (r runner) Run(ctx context.Context, hash uint64, input []byte) ([]byte, error) {
	return r(ctx, hash, input)
}

type hasher interface {
	Hash() uint64
}
