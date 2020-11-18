## redis-proxy

A redis proxy middleware implemented in go language, supports sharding by key

It only implements basic functions, just for testing and verification, without considering performance, and many places are imperfect


## build

```bash
cd cmd/redis-proxy
go build -v .
```

## run

```bash
./redis-proxy // listen at 6380 port

or 
./redis-proxy --port=6380 --cluster-addr=localhost:7000,localhost:7001
```

## test

```
redis-cli -p 6380
or 
redis-benchmark -p 6380 -n 1000 -c 20
```