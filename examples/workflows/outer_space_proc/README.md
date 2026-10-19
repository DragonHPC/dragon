# Dragon Outer-Space Process Examples

This directory contains examples for processes that interact with a Dragon
runtime without being launched as ordinary Dragon-managed processes. The
tracked examples cover:

- a Python process that starts and attaches to a local runtime;
- a Python process that attaches to an already-running co-located runtime;
- proxy access to a runtime on another system;
- a standalone C++ process using `dragon_ext_attach()`; and
- a Slurm scaling run that measures native attach latency.

## Prerequisites

Build Dragon and activate the development environment from the repository
root:

```bash
source devtools/setup
```

The `dragon`, `dragon-config`, and Python commands used below must resolve from
that environment. Commands in this README assume the repository root is the
current directory unless stated otherwise.

## Python Workload

[`client.py`](client.py) uses the same workload in its local and proxy modes:

1. Create a `dragon.native.queue.Queue`.
2. Put the first line from [`scroll.py`](scroll.py) into the queue.
3. Start a `dragon.native.process.Process` that reads the first line and puts
   the second line back.
4. Read and verify the second line in the external client.

### Start and Attach From One Process

The `--start` mode calls `LocalRuntime.start()`. It starts an attachable
single-node runtime subprocess, attaches the client, runs the workload, and
stops the runtime in a `finally` block.

```bash
cd examples/workflows/outer_space_proc
python client.py --start
```

### Attach to an Existing Runtime

Use two terminals. Both terminals must have the Dragon environment activated.

In terminal 1, launch an attachable runtime:

```bash
cd examples/workflows/outer_space_proc
dragon -a dragon-waiting-runtime-head
```

The launcher prints an attach token. In terminal 2, export that exact token
before starting the external client:

```bash
cd examples/workflows/outer_space_proc
export DRAGON_ATTACH_TOKEN="<attach token printed in terminal 1>"
python client.py --attach
```

`--attach` is the default mode, so `python client.py` is equivalent. The
client reads the standard co-located attach-info file, authenticates with the
exported token, runs the workload, and calls `LocalRuntime.stop()` to signal
runtime shutdown through Dragon's registered control event.

[`run_interactive_demo.sh`](run_interactive_demo.sh) starts the same
`dragon -a dragon-waiting-runtime-head` command in the background and waits
for it to exit. When using that helper, the second terminal must still export
the token printed in the first terminal before running `client.py --attach`.

## Remote Proxy Example

[`remote-server.py`](remote-server.py) publishes a runtime descriptor and
waits for an exit marker. Launch it under Dragon on the remote system:

```bash
cd /absolute/path/to/examples/workflows/outer_space_proc
dragon remote-server.py \
    --name my-runtime \
    --publish-path "$PWD" \
    --exit-path "$PWD/client_exit"
```

On the client system, run:

```bash
cd examples/workflows/outer_space_proc
python client.py --proxy \
    --system=<remote-host> \
    --runtime-name=my-runtime \
    --remote-working-dir=/absolute/remote/path/to/examples/workflows/outer_space_proc \
    --exit-path=/absolute/remote/path/to/examples/workflows/outer_space_proc/client_exit
```

This path uses `lookup()` to retrieve the published descriptor, creates a
`Proxy`, activates it for the workload, and then writes the remote exit marker.
SSH/SCP access to the remote system is required.

[`run_server.sh`](run_server.sh) and [`run_proxy_demo.sh`](run_proxy_demo.sh)
are site-specific command templates. In particular, `run_proxy_demo.sh`
contains a fixed host and filesystem path and must be adapted before use on
another system.

## Native C++ Attach

[`ext_attach_publisher.py`](ext_attach_publisher.py) creates two Python
`Queue` objects and publishes their serialized descriptors through the Local
Services key-value store. [`ext_attach_client.cpp`](ext_attach_client.cpp) is
a standalone executable that attaches with `dragon_ext_attach()`, retrieves
the descriptors, receives bytes from the first queue, replies on the second,
and detaches with `dragon_ext_detach()`.

Build the client with the tracked [`Makefile`](Makefile):

```bash
cd examples/workflows/outer_space_proc
make
```

Then use two terminals with the same explicit token.

Terminal 1:

```bash
cd examples/workflows/outer_space_proc
export DRAGON_ATTACH_TOKEN="choose-a-secret-token"
dragon -a ext_attach_publisher.py
```

Terminal 2, after the publisher reports that it is ready:

```bash
cd examples/workflows/outer_space_proc
export DRAGON_ATTACH_TOKEN="choose-a-secret-token"
./ext_attach_client
```

The executable is a build product and is not tracked. The publisher and
client exchange raw byte payloads because the standalone C++ process does not
decode Python pickle payloads.

## Slurm Attach-Latency Run

[`run_ext_attach_srun_demo.sh`](run_ext_attach_srun_demo.sh) runs the native
attach example across an existing Slurm allocation. It builds the C++ client,
launches the publisher with `--srun`, starts 128 client tasks per node, and
writes attach-time statistics after all ranks reply.

Run it inside an allocation where `SLURM_JOB_ID` is set after running `dhosts --wlm slurm --print-only` and exporting the environment variable that is printed out:

```bash
cd examples/workflows/outer_space_proc
./run_ext_attach_srun_demo.sh
```

The task count is fixed by `SRUN_PROCS_PER_NODE` in
`ext_attach_publisher.py` and the matching `--ntasks-per-node` setting in the
shell script.

[`attach_delay_analysis.py`](attach_delay_analysis.py) plots timing files
produced by these runs:

```bash
python attach_delay_analysis.py --input-dir <results-dir> --output attach_delay_boxplot.png
```

An example tracked plot is available as
[`attach_delay_boxplot.png`](attach_delay_boxplot.png).

## Tracked File Reference

| File | Purpose |
|---|---|
| [`client.py`](client.py) | Python client with `--start`, `--attach`, `--proxy`, and `--outside` modes. |
| [`scroll.py`](scroll.py) | Shared payload strings used by the Python and C++ round trips. |
| [`run_interactive_demo.sh`](run_interactive_demo.sh) | Starts an attachable runtime and waits while a second terminal runs the client. |
| [`remote-server.py`](remote-server.py) | Publishes a runtime descriptor for proxy clients and waits for an exit marker. |
| [`run_server.sh`](run_server.sh) | Site-oriented wrapper for `remote-server.py`. |
| [`run_proxy_demo.sh`](run_proxy_demo.sh) | Site-specific proxy-client command template. |
| [`ext_attach_publisher.py`](ext_attach_publisher.py) | Publishes queues for native external clients and records scaled timing results. |
| [`ext_attach_client.cpp`](ext_attach_client.cpp) | Standalone C++ attach and queue round-trip client. |
| [`Makefile`](Makefile) | Builds the native C++ client with `dragon-config`. |
| [`run_ext_attach_srun_demo.sh`](run_ext_attach_srun_demo.sh) | Runs the native attach latency workload under Slurm. |
| [`attach_delay_analysis.py`](attach_delay_analysis.py) | Generates a box plot from attach timing results. |
| [`attach_delay_boxplot.png`](attach_delay_boxplot.png) | Example attach-latency plot. |
| [`README.md`](README.md) | This guide. |

## Troubleshooting

### No Attach Token

`client.py --attach` and `ext_attach_client` require
`DRAGON_ATTACH_TOKEN` in the external process environment. Export the token
printed by the attachable launcher, or use the same explicit token in both
terminals for the native example.

### No Attach-Info File

The local client uses the standard attach-info location for the current user.
Start the runtime with `dragon -a` and wait for launcher startup to complete
before running the client. If the client still cannot find the file, set
`DRAGON_ATTACH_DIR` to the same directory for both the runtime and the client.

### Native Client Cannot Find Queue Keys

Wait until `ext_attach_publisher.py` reports readiness before running the C++
client. Runtime attach-info becomes available before the publisher finishes
disseminating its queue descriptors.

### Proxy Lookup Fails

Confirm that the descriptor named by `--runtime-name` exists in the remote
publish directory and that the client can reach the remote host with SSH and
SCP. Use absolute remote paths for `--remote-working-dir` and `--exit-path`.
