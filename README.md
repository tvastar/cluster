# cluster

[![Status](https://travis-ci.com/tvastar/cluster.svg?branch=master)](https://travis-ci.com/tvastar/cluster?branch=master)
[![GoDoc](https://godoc.org/github.com/tvastar/cluster?status.svg)](https://godoc.org/github.com/tvastar/cluster)
[![codecov](https://codecov.io/gh/tvastar/cluster/branch/master/graph/badge.svg)](https://codecov.io/gh/tvastar/cluster)
[![Go Report Card](https://goreportcard.com/badge/github.com/tvastar/cluster)](https://goreportcard.com/report/github.com/tvastar/cluster)

Librarie for building distributed services

## Partition

The [partition](https://godoc.org/github.com/tvastar/cluster/pkg/partition) package supports self-sharding: where a cluster of
servers accept any request but then hash the request and route it to
one of the servers in the request based on the hash.

This combines the "gateway which shards the request to servers" with
the "servers which serve a shard" into one single cluster.

Note that this sharding is not perfect but it is quite useful when
sharding improves performance (by caching requests) or allows serial
execution (to avoid redoing some work).  This is not useful when
sharding is needed for correctness (that requires some form of
distributed locking).

