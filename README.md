# A simple io_uring tcp server with custom tcp protocol

```sh
$ sudo apt-get update && sudo apt-get install -y build-essential cmake liburing-dev
$ cmake -S . -B build
$ cmake --build build
$ ./build/server
```