# a simple key value store built on top of viewstamped replication revisited paper

```sh
$ sudo apt-get update && sudo apt-get install -y build-essential cmake liburing-dev
$ cmake -S . -B build
$ cmake --build build
```

- Run Replica 0: `Open terminal 1: ./build/server 0`
- Run Replica 1: `Open terminal 2: ./build/server 1`
- Run Replica 2: `Open terminal 3: ./build/server 2`
- Run Client : `Open another terminal : ./build/client`
