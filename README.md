# SEN-STORE

LevelDB based Key-Value store for IoT gateway

# Building

This project supports [CMake](https://cmake.org/) out of the box.

### Build for POSIX

Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```

# Run TPCxIoT Benchmark

```bash
cd script
sh run-tpcxiot.sh
```

# Limitations

  * There is no client-server support builtin to the library.  An application that needs such support will have to wrap their own server around the library.
 

