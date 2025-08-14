# a simple key value store built on top of viewstamped replication revisited paper

```sh
$ sudo apt-get update && sudo apt-get install -y build-essential cmake liburing-dev
$ cmake -S . -B build
$ cmake --build build
```

- Run Replica 0: `Open terminal 1: ./build/server 0`
- Run Replica 1: `Open terminal 2: ./build/server 1`
- Run Replica 2: `Open terminal 3: ./build/server 2`
- Run Replica 3: `Open terminal 4: ./build/server 3`
- Run Replica 4: `Open terminal 5: ./build/server 4`
- Run Client : `Open another terminal : ./build/client`
