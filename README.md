TimeSeries Parser
===

## Overview

This application will process input files, split them into partition files and store them in disk.

HTTP server will start after the processing is done and will handle these partition files on parsing requests.

Accepts POST requests to `/`, other paths will throw 404

## Partition overview

Partition consists data and meta information.

### Meta

Meta information tells `MinTimestamp` and `MaxTimestamp` for this particular data and is loaded into memory(also stored as a separate file on disk).

This way we can determine if we should consider this partition for parsing and avoid unnecessary partition processing on parsing request.

### Data

Data is stored as a msgpack-encoded file on disk, each record sorted by timestamp

Also partition object uses mmap syscall to map file into a byte slice


## Parsing methodology

Since partitions are built from time-sorted original file, resulting partition list is sorted as well. 

We can use binary search to find the first and the last partition to consider as each partition has `MinTimestamp` and `MaxTimestamp`

Also we can use binary search inside a partition to retrieve records.

## Testing

Unit tests could be run with script `runtests.sh` or manually. Note that some tests require generated mocks,
to generate mocks, run 
```bash
mkdir mocks
mockgen -destination=mocks/mock_partition.go -package=mocks github.com/ssfilatov/ts/pkg/partition Partition
```

to run tests
```bash
go test ./...
```

## Performance

Pprof routing is added to main http server, to use pprof, run
```bash
go tool pprof main http://127.0.0.1:8279/debug/pprof/profile
go tool pprof main http://127.0.0.1:8279/debug/pprof/heap
```
etc.

wrk is also used for benchmarking, `post.lua` - example payload

## Things to improve

### JSON encoding

Originally I used json.Encoder streaming and applied streaming writes into a socket. 
It was quite efficient retrieving huge documents, but could not make a json array from it :)

Anyway external libraries without reflection usage are more performant, I took easyjson for that purpose.
I used it the same way streaming does it, but it looks ugly and I'm pretty sure it could be made better.

### Msgpack encoding

The point I already mentioned - default encoder uses reflection which is not performant

### Partition internals

I'm pretty sure partition data could be compressed and stored more efficiently. 

Also some parameters could be tweaked like fitting partition into a page cache.

### Partition processing

Right now server doesn't start until all files are processed. 
This is inconvenient and could be changed regarding to our needs.
Also processing time is huge - I guess it could be changed with concurrency.

### Logging and metrics

There are no metrics and now and logging is poor, this doesn't stand for production-grade service.

Some prometheus exporters and extensive debug logging could be added.

### Graceful shutdown

Right now server handles SIGTERM gracefully but other parts of code don't

### Some frameworks

I tried to keep the code simple without a lot of external dependecies. 

But default implementations of http server, logging, errors are not the most performant and useful.
