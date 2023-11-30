# Overview
Code to investigate partial aggregates using Aparche Arrow's compute API.

# Execution
The compilation section, above, describes how to compile the source files into executable
binaries. The ">> Executables" section defines the executables that will be created;
specifically, we expect the executable `experiment-arrowcompute` to exist after successful
compilation:

```bash
# Execute the binary so that it runs the aggregation function on the test data, in memory as a
# single table
time ./build-dir/experiment-arrowcompute "$PWD" table

# Execute the binary so that it applies the aggregation function on every "chunk" of the table
# as a separate table. We don't time any of the conversion, just the compute portion.
time ./build-dir/experiment-arrowcompute "$PWD" vector
```

# Performance
I am interested in partial aggregates and applying them on distinct devices, environments,
or on data that is retrieved from distinct devices and environments.

## System Info

Using ArchLinux when the experiment was first run:
```bash
>> uname -a
Linux <hostname> 5.16.10-arch1-1 #1 SMP PREEMPT Wed, 16 Feb 2022 19:35:18 +0000 x86_64 GNU/Linux
```

Using ArchLinux after new hardware:
```bash
>> uname -a
Linux <hostname> 6.6.2-arch1-1 #1 SMP PREEMPT_DYNAMIC Mon, 30 Nov 2023 23:18:21 +0000 x86_64 GNU/Linux
```

Arrow can be installed by using a modified version of the PKGBUILD file available in the
`resources/` directory (original available here: [PKGBUILD][archgit-arrow]). This should
be updated to version 12.0.1, but the key reason to customize the PKGBUILD is simply to be
sure that it builds with particular options. Otherwise, it's possible to simply install
using pacman:
```bash
sudo pacman -S arrow
```

Some key information from the PKGBUILD (IMO):
```
  cmake                                                \
    -B build -S apache-${pkgname}-${pkgver}/cpp        \
    ...
    -DCMAKE_BUILD_TYPE=Release                         \
    -DARROW_DEPENDENCY_SOURCE=SYSTEM                   \
    -DARROW_BUILD_STATIC=ON                            \
    -DARROW_BUILD_SHARED=ON                            \
    -DARROW_COMPUTE=ON                                 \
    -DARROW_FLIGHT=ON                                  \
    -DARROW_GANDIVA=ON                                 \
    -DARROW_IPC=ON                                     \
    -DARROW_PYTHON=ON                                  \
    -DARROW_SIMD_LEVEL=AVX2                            \
    ...                                                \
  make -C build
```

My gcc and LLVM versions at the time the experiment was first run:
```bash
>> g++ --version
gcc (GCC) 11.2.0

>> clang++ --version
clang version 13.0.1
Target: x86_64-pc-linux-gnu
Thread model: posix
```

My gcc and LLVM versions after new hardware:
```bash
>> g++ --version
gcc (GCC) 13.2.1 20230801

>> clang++ --version
clang version 16.0.6
Target: x86_64-pc-linux-gnu
Thread model: posix
```

Some processor info at the time the experiment was originally run:
```bash
>> dmidecode -t 4
# ...
	SSE (Streaming SIMD extensions)
	SSE2 (Streaming SIMD extensions 2)
# ...
Version: Intel(R) Xeon(R) CPU E3-1270 v3 @ 3.50GHz
Core Count: 4
Core Enabled: 4
Thread Count: 8
# ...
```

Processor info after new hardware:
```bash
>> dmidecode -t 4
# ...
	SSE (Streaming SIMD extensions)
	SSE2 (Streaming SIMD extensions 2)
# ...
Version: AMD Ryzen 9 5950X 16-Core Processor
Max Speed: 5050 MHz
Current Speed: 3400 MHz
Core Count: 16
Core Enabled: 16
Thread Count: 32
# ...
```


## Results
For the first table-based scenario, the timing I see is as follows (NOTE: "..." replace
the path to my home directory).

At the time the experiment was first run:
```bash
>>  time ./build-dir/experiment-arrowcompute "$PWD" table
Using directory  [.../code/misc/cpp/arrow-compute-perf]
Aggregating over [table]
Parsing file: 'file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather'
Reading arrow IPC-formatted file: file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather
Table dimensions [27118, 2152]
        [565] chunks
        [48] chunk_size
Aggr Time:      2240ms

________________________________________________________
Executed in    4.08 secs    fish           external
   usr time    3.81 secs  500.00 micros    3.81 secs
   sys time    0.28 secs   98.00 micros    0.28 secs
```

After new hardware:
```bash
>>  time ./build-dir/experiment-arrowcompute "$PWD" table
Using directory  [.../code/research/skytether-experiments/arrow-compute-inmem]
Aggregating over [table]
Parsing file: 'file://.../skytether-experiments/arrow-compute-inmem/resources/E-GEOD-76312.48-2152.x565.feather'
Reading arrow IPC-formatted file: file://.../skytether-experiments/arrow-compute-inmem/resources/E-GEOD-76312.48-2152.x565.feather
Table dimensions [27118, 2152]
        [565] chunks
        [48] chunk_size
Aggr Time:      328ms

________________________________________________________
Executed in    986.69 millis    fish           external
   usr time    696.20 millis  332.00 micros    695.87 millis
   sys time    289.66 millis   47.00 micros    289.61 millis
```


For the second "slice-based" or "vector-based" approach, the timing I see is as follows.

At the time the experiment was first run:
```bash
>> time ./build-dir/experiment-arrowcompute "$PWD" table
Using directory  [.../code/misc/cpp/arrow-compute-perf]
Aggregating over [table]
Parsing file: 'file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather'
Reading arrow IPC-formatted file: file://.../code/misc/cpp/arrow-compute-perf/resources/E-GEOD-76312.48-2152.x565.feather
Table dimensions [27118, 2152]
        [565] chunks
        [48] chunk_size
Aggr Time:      55514ms

________________________________________________________
Executed in   58.67 secs    fish           external
   usr time   58.30 secs  502.00 micros   58.30 secs
   sys time    0.30 secs   96.00 micros    0.30 secs
```

After new hardware:
```bash
# Note: "..." is used in place of the path of my home directory for this output

>> time ./build-dir/experiment-arrowcompute "$PWD" vector
Using directory  [.../code/research/skytether-experiments/arrow-compute-inmem]
Aggregating over [vector]
Parsing file: 'file://.../skytether-experiments/arrow-compute-inmem/resources/E-GEOD-76312.48-2152.x565.feather'
Reading arrow IPC-formatted file: file://.../skytether-experiments/arrow-compute-inmem/resources/E-GEOD-76312.48-2152.x565.feather
Table dimensions [27118, 2152]
        [565] chunks
        [48] chunk_size
Row count for most slices: [48]
Row count for final slice: [46]
Aggr Time:      23719ms
      Avg:         41ms

________________________________________________________
Executed in   24.96 secs    fish           external
   usr time   24.66 secs  345.00 micros   24.66 secs
   sys time    0.27 secs   52.00 micros    0.30 secs
```


<!-- resources -->
[archgit-arrow]: https://gitlab.archlinux.org/archlinux/packaging/packages/arrow
