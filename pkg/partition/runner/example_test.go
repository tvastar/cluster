// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package runner_test

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/alicebob/miniredis"
	"github.com/tvastar/cluster/pkg/partition"
	"github.com/tvastar/cluster/pkg/partition/runner"
)

func Example() {
	opt, cleanup := exampleSetup()
	defer cleanup()

	h := runner.Handlers{}
	intf := h.Register(func(ctx context.Context, r IntRequest) (string, error) {
		return "hello", nil
	}).(func(ctx context.Context, r IntRequest) (string, error))

	strf := h.Register(func(ctx context.Context, r StringRequest) (string, error) {
		return string(r), nil
	}).(func(ctx context.Context, r StringRequest) (string, error))

	h.Start(context.Background(), ":2000", opt)

	s, err := intf(context.Background(), IntRequest(0))
	fmt.Println("Got", s, err)

	s, err = strf(context.Background(), StringRequest("boo"))
	fmt.Println("Got", s, err)

	// Output:
	// Got hello <nil>
	// Got boo <nil>

}

type IntRequest int

func (i IntRequest) Hash() uint64 {
	return 0
}

type StringRequest string

func (s StringRequest) Hash() uint64 {
	return 5
}

func exampleSetup() (partition.Option, func()) {
	gob.Register(IntRequest(0))
	gob.Register(StringRequest("hello"))

	minir, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opt := partition.WithEndpointRegistry(partition.NewRedisRegistry(minir.Addr(), "prefix_"))

	return opt, func() { minir.Close() }
}
