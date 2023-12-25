#!/bin/bash -ex

# generate easyjson
easyjson -all pkg/record

# generate mocks
mkdir mocks
mockgen -destination=mocks/mock_partition.go -package=mocks github.com/ssfilatov/ts/pkg/partition Partition

go test ./...

# clean
rm -rf mocks
