// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/go-redis/redis/v7"
)

// NewRedisRegistry returns a new registry based on Redis
//
// Note that this fetches the endpoint from redis on every call
// A wrapper implementation can easily override this behaviour by
// caching the results of the last call to ListEndpoints.
func NewRedisRegistry(addr string, prefix string) EndpointRegistry {
	ttl := time.Minute
	return &redisreg{redis.NewClient(&redis.Options{Addr: addr}), prefix, ttl}
}

type redisreg struct {
	*redis.Client
	prefix string
	ttl    time.Duration
}

func (r *redisreg) RegisterEndpoint(ctx context.Context, addr string) (io.Closer, error) {
	err := r.addEndpoint(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go r.refreshLoop(ctx, addr)
	return cancelcloser(cancel), nil
}

func (r *redisreg) ListEndpoints(ctx context.Context, refresh bool) ([]string, error) {
	return r.listEndpoints()
}

func (r *redisreg) refreshLoop(ctx context.Context, addr string) {
	defer r.Client.Close()
	defer r.removeEndpoint(addr)

	timer := time.NewTimer(r.ttl / 3)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		if err := r.addEndpoint(addr); err != nil {
			// report error better
			log.Println("unexpected redis err", err)
		}
	}
}

func (r *redisreg) addEndpoint(addr string) error {
	expires := float64(time.Now().Add(r.ttl).Unix())
	_, err := r.ZAdd(r.prefix+"endpoints", &redis.Z{Score: expires, Member: addr}).Result()
	return err
}

func (r *redisreg) removeEndpoint(addr string) {
	if _, err := r.ZRem(r.prefix+"endpoints", addr).Result(); err != nil {
		log.Println("Unexpected redis err removing endpoint", err)
	}
}

func (r *redisreg) listEndpoints() ([]string, error) {
	rangeBy := redis.ZRangeBy{
		Min: fmt.Sprint(time.Now().Unix()),
		Max: "Inf",
	}

	return r.ZRangeByScore(r.prefix+"endpoints", &rangeBy).Result()
}

func (r *redisreg) purgeEndpoints() {
	expires := fmt.Sprint(time.Now().Unix())
	_, err := r.ZRemRangeByScore(r.prefix+"endpoints", "0", expires).Result()
	if err != nil {
		log.Println("Unexpected redis err removing stale endpoints", err)
	}
}

type cancelcloser func()

func (c cancelcloser) Close() error {
	c()
	return nil
}
