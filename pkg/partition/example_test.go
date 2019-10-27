// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/tvastar/cluster/pkg/partition"
)

func Example() {
	minir, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer minir.Close()

	opt1 := partition.WithEndpointRegistry(partition.NewRedisRegistry(minir.Addr(), "prefix_"))
	opt2 := partition.WithEndpointRegistry(partition.NewRedisRegistry(minir.Addr(), "prefix_"))

	ctx := context.Background()
	h1 := handler(1)
	h2 := handler(2)

	part1, err := partition.New(ctx, ":2222", h1, opt1)
	if err != nil {
		panic(err)
	}
	defer part1.Close()

	part2, err := partition.New(ctx, ":2223", h2, opt2)
	if err != nil {
		panic(err)
	}
	defer part2.Close()

	// wait for both endpoints to be registered
	time.Sleep(100 * time.Millisecond)

	if _, err := part1.Run(ctx, 555, []byte("hello")); err != nil {
		panic(err)
	}

	if _, err := part2.Run(ctx, 555, []byte("hello")); err != nil {
		panic(err)
	}

	if _, err := part1.Run(ctx, 22222, []byte("hello")); err != nil {
		panic(err)
	}

	if _, err := part2.Run(ctx, 22222, []byte("hello")); err != nil {
		panic(err)
	}

	// Output:
	// [1] Run(555, hello)
	// [1] Run(555, hello)
	// [2] Run(22222, hello)
	// [2] Run(22222, hello)
}

type handler int

func (h handler) Run(ctx context.Context, hash uint64, input []byte) ([]byte, error) {
	fmt.Printf("[%d] Run(%d, %s)\n", int(h), hash, string(input))
	return input, nil
}

func (h handler) Close() error {
	return nil
}
