import argparse
from argparse import RawTextHelpFormatter
import json
import os
import sys
from enum import Enum, auto
from pathlib import Path

EXPLICIT_COMPILE_HELP = (
    """With brief description, print the compilation and link options for building C programs with Dragon and exit"""
)
LINKER_HELP = (
    """For execution during linking, print the linker option for build applications built against Dragon C/C++ API"""
)
COMPILER_HELP = """For execution during compilation, print the compiler option for building applications built against Dragon C/C++ API"""

DRAGON_CONFIG_ADD_HELP = """Define a number of paths (key=value) to configure
include and library paths for Dragon, or to make the TCP runtime the always-on
default for backend communication (set to True).

Examples
--------
UCX backend:
dragon-config add --ucx-include=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/include
dragon-config add --ucx-build-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib
dragon-config add --ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib

Set TCP transport as always-on default backend:
dragon-config add --tcp-runtime=True

Set PMIx header files location to enable PMIx support for MPI applications. Specifically looking for
path <pmix include>/src/include/pmix_globals.h
dragon-config add --pmix-include=/usr/include:/usr/include/pmix

"""

DRAGON_CONFIG_TEST_HELP = """Define paths necessary for executing tests of Dragon's MPI application support

Examples
--------
Set paths for headers and libraries for Cray MPICH, Open MPI, or ANL MPICH installations.
dragon-config test --cray-mpich=/opt/cray/pe/lmod/modulefiles/comnet/gnu/12.0/ofi/1.0/cray-mpich
dragon-config test --open-mpi=/lus/scratch/dragonhpc/openmpi
dragon-config test --anl-mpich=/lus/scratch/dragonhpc/mpich
"""

SERIALIZE_HELP = """Serialize all key-value pairs currently in the configuration file into a single,
colon-separated string that can be passed to the --add command.
"""

DRAGON_NETWORK_MPIEXEC_HELP = """Add mpiexec override commands for Dragon's PBS+PALS launcher. This is used to add overrides
for the mpiexec commands used to launch the network config tool and thedeprecated cleanup processes.
The command needs to launch one process per node, line buffer the output, and tag the output with
the process rank with some unique identifying information (global rank, hostname, etc). The commands
should be passed as a single string. The following special strings are necessary and will be
automatically filled in at the time of use by Dragon:

{nnodes} = number of nodes

Examples
--------
Set launcher mpiexec network config override for Cray-PALS:
$ dragon-config add --netconfig-mpiexec-override='mpiexec --np {nnodes} -ppn 1 -l --line-buffer'

Set launcher mpiexec network config override for OpenMPI 5.0.6:
$ dragon-config add --netconfig-mpiexec-override='mpiexec --np {nnodes} --map-by ppr:1:node --stream-buffering=1 --tag-output'

These commands are used by default when the dragon launcher detects PBS+PALS.

To avoid checks with the automatic wlm detection and utilize the overriden mpiexec commands, run dragon with the
workload manager specified as '--wlm=pbs+pals'.
"""

DRAGON_BACKEND_MPIEXEC_HELP = """Add mpiexec override commands for Dragon's PBS+PALS launcher. This is used to add overrides
for the mpiexec commands used to launch the backend processes. The command should be passed as a
single string. The following special strings are necessary and will be automatically dilled in at
the time of use by Dragon:

{nodes} = number of nodes, {nodelist} = comma separated list of nodes

Examples
--------
Set launcher mpiexec override for Cray-PALS:
$ dragon-config add --backend-mpiexec-override='mpiexec --np {nnodes} --ppn 1 --cpu-bind none --hosts {nodelist} --line-buffer'

Set launcher mpiexec network config override for OpenMPI 5.0.6:
$ dragon-config add --backend-mpiexec-override='mpiexec --np {nnodes} --map-by ppr:1:node --stream-buffering=1 --tag-output --host {nodelist}'

These commands are used by default when the dragon launcher detects PBS+PALS.

To avoid checks with the automatic wlm detection and utilize the overriden mpiexec commands, run dragon
with the workload manager specified as '--wlm=pbs+pals'.
"""

GET_KV_HELP = """Get value for given key that can be passed to the --add or --add-mpiexec command.
"""

# Define all the keys for add entries:
ADD_KEYS = {
    "ofi_include",
    "ofi_build_lib",
    "ofi_runtime_lib",
    "ucx_include",
    "ucx_build_lib",
    "ucx_runtime_lib",
    "pmix_include",
    "mpi_include",
    "cuda_include",
    "cuda_runtime_lib",
    "hip_include",
    "ze_include",
    "tcp_runtime",
    "netconfig_mpiexec_override",
    "backend_mpiexec_override",
}

TEST_KEYS = {"cray_mpich", "open_mpi", "anl_mpich"}

ROUTING_ENUM = Enum("Routing", {name: auto() for name in ADD_KEYS | TEST_KEYS})

_ATBL = {}


def config_router(routing_key, routing_table, metadata=None):
    """Decorator routing adapter.

    This is largely a copy of the router in dragon.infrasturcture.util.route, but
    placed here to avoid trying to import dragon before we've actually built it, which
    is a problem for dragon-config that's to be avoided

    :param msg_type: the type of an infrastructure message class
    :param routing_table: dict, indexed by msg_type with values a tuple (function, metadata)
    :param metadata: metadata to check before function is called, usage dependent
    :return: the function.
    """

    def decorator_route(f):
        assert routing_key not in routing_table
        routing_table[routing_key] = (f, metadata)
        return f

    return decorator_route


def _update_json_config(config_filename, key, val):
    """Update the json config file loaded during runtime execution"""

    if os.path.isfile(config_filename):
        with open(config_filename) as config_file:
            config_dict = json.load(config_file)
    else:
        config_dict = {}

    # if key already is in dict, append it into a list
    if key in config_dict:
        if isinstance(config_dict[key], list):
            if val not in config_dict[key]:
                config_dict[key].append(val)
        else:
            cur = config_dict[key]
            if val != cur:
                config_dict[key] = [cur, val]
    else:
        new_env = {key: val}
        config_dict.update(new_env)

    with open(config_filename, "w") as config_file:
        json.dump(config_dict, config_file)


@config_router(ROUTING_ENUM.ofi_include, _ATBL)
def _ofi_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    header = path / "rdma" / "fabric.h"
    if not os.path.isfile(header):
        print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_OFI_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.ofi_include.name, str(path))


@config_router(ROUTING_ENUM.ucx_include, _ATBL)
def _ucx_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    header = path / "ucp" / "api" / "ucp.h"
    if not os.path.isfile(header):
        print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_UCX_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.ucx_include.name, str(path))


@config_router(ROUTING_ENUM.cuda_include, _ATBL)
def _cuda_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    header = path / "cuda_runtime_api.h"
    if not os.path.isfile(header):
        print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/cuda.cpp\n")
    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_CUDA_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.cuda_include.name, str(path))


@config_router(ROUTING_ENUM.hip_include, _ATBL)
def _hip_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    header = path / "hip" / "hip_runtime_api.h"
    if not os.path.isfile(header):
        print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/hip.cpp\n")
    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_HIP_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.hip_include.name, str(path))


@config_router(ROUTING_ENUM.ze_include, _ATBL)
def _ze_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    header = path / "ze_api.h"
    if not os.path.isfile(header):
        print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/ze.cpp\n")
    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_ZE_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.ze_include.name, str(path))


@config_router(ROUTING_ENUM.mpi_include, _ATBL)
def _mpi_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    header = path / "mpi.h"
    if not os.path.isfile(header):
        print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_MPI_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.mpi_include.name, str(path))


@config_router(ROUTING_ENUM.pmix_include, _ATBL)
def _pmix_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    global_header = path / "src/include/pmix_globals.h"
    common_header = path / "pmix_common.h"
    event_header = path / "event.h"
    if not os.path.isfile(global_header) and not os.path.isfile(common_header) and not os.path.isfile(event_header):
        print(
            f"{global_header}, {common_header}, nor {event_header} exist, make sure file paths have been set correctly",
            flush=True,
        )
        sys.exit(1)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_PMIX_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.pmix_include.name, str(path))


@config_router(ROUTING_ENUM.ofi_runtime_lib, _ATBL)
def _ofi_runtime_lib(make_file, config_filename, path, base_dir):
    _update_json_config(config_filename, ROUTING_ENUM.ofi_runtime_lib.name, str(path))


@config_router(ROUTING_ENUM.ucx_runtime_lib, _ATBL)
def _ucx_runtime_lib(make_file, config_filename, path, base_dir):
    _update_json_config(config_filename, ROUTING_ENUM.ucx_runtime_lib.name, str(path))


@config_router(ROUTING_ENUM.cuda_runtime_lib, _ATBL)
def _cuda_runtime_lib(make_file, config_filename, path, base_dir):
    _update_json_config(config_filename, ROUTING_ENUM.cuda_runtime_lib.name, str(path))


@config_router(ROUTING_ENUM.ofi_build_lib, _ATBL)
def _ofi_build_lib_route(make_file, config_filename, path, base_dir):
    # sanity check
    lib = path / "libfabric.so"
    if not os.path.isfile(lib):
        print(f"{lib} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write("CONFIG_OFI_LIBS := \n")
    make_file.write("CONFIG_BACKEND_LIBS := $(CONFIG_BACKEND_LIBS) -L. -lfabric\n")
    make_file.write("CONFIG_BACKEND_LIB_DEPS := $(CONFIG_BACKEND_LIB_DEPS) libdfabric_ofi.so\n")

    _update_json_config(config_filename, ROUTING_ENUM.ofi_build_lib.name, str(path))


@config_router(ROUTING_ENUM.ucx_build_lib, _ATBL)
def _ucx_build_lib_route(make_file, config_filename, path, base_dir):
    # sanity check
    lib = path / "libucp.so"
    if not os.path.isfile(lib):
        print(f"{lib} does not exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write("CONFIG_UCX_LIBS := \n")
    make_file.write("CONFIG_BACKEND_LIBS := $(CONFIG_BACKEND_LIBS) -L. -lucp\n")
    make_file.write("CONFIG_BACKEND_LIB_DEPS := $(CONFIG_BACKEND_LIB_DEPS) libdfabric_ucx.so\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.ucx_build_lib.name, str(path))


@config_router(ROUTING_ENUM.cray_mpich, _ATBL)
def _cray_mpich_test_route(make_file, config_filename, path, base_dir):
    # sanity check
    libname = "mpich"

    lib_path = path / "lib"
    lib = lib_path / f"lib{libname}.so"

    lib64_path = path / "lib64"
    lib64 = lib64_path / f"lib{libname}.so"

    if not os.path.isfile(lib) and not os.path.isfile(lib64):
        print(f"{lib} nor {lib64} exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    if os.path.isfile(lib64):
        lib_path = lib64_path

    include_path = path / "include"
    if not os.path.isfile(include_path / "mpi.h"):
        print(f"{include_path / 'mpi.h'} does not  exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write(f"TEST_CRAY_MPICH_FLAGS := -I {include_path} -L {lib_path} -l{libname}\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.cray_mpich.name, str(path))
    _update_json_config(config_filename, ROUTING_ENUM.cray_mpich.name + "_runtime_lib", str(lib_path))


@config_router(ROUTING_ENUM.open_mpi, _ATBL)
def _open_mpi_test_route(make_file, config_filename, path, base_dir):
    # sanity check
    libname = "mpi"

    lib_path = path / "lib"
    lib = lib_path / f"lib{libname}.so"

    lib64_path = path / "lib64"
    lib64 = lib64_path / f"lib{libname}.so"

    if not os.path.isfile(lib) and not os.path.isfile(lib64):
        print(f"{lib} nor {lib64} exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    if os.path.isfile(lib64):
        lib_path = lib64_path

    include_path = path / "include"
    if not os.path.isfile(include_path / "mpi.h"):
        print(f"{include_path / 'mpi.h'} does not  exist, make sure file paths have been set correctly", flush=True)
        return
    make_file.write(f"TEST_OPEN_MPI_FLAGS := -I {include_path} -L {lib_path} -l{libname}\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.open_mpi.name, str(path))
    _update_json_config(config_filename, ROUTING_ENUM.open_mpi.name + "_runtime_lib", str(lib_path))


@config_router(ROUTING_ENUM.anl_mpich, _ATBL)
def _anl_mpich_test_route(make_file, config_filename, path, base_dir):
    # sanity check
    libname = "mpich"

    lib_path = path / "lib"
    lib = lib_path / f"lib{libname}.so"

    lib64_path = path / "lib64"
    lib64 = lib64_path / f"lib{libname}.so"

    if not os.path.isfile(lib) and not os.path.isfile(lib64):
        print(f"{lib} nor {lib64} exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    if os.path.isfile(lib64):
        lib_path = lib64_path

    include_path = path / "include"
    if not os.path.isfile(include_path / "mpi.h"):
        print(f"{include_path / 'mpi.h'} does not  exist, make sure file paths have been set correctly", flush=True)
        sys.exit(1)

    make_file.write(f"TEST_ANL_MPICH_FLAGS := -I {include_path} -L {lib_path} -l{libname}\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.anl_mpich.name, str(path))
    _update_json_config(config_filename, ROUTING_ENUM.anl_mpich.name + "_runtime_lib", str(lib_path))


def hugepages_cleanup(careful_restart_cleanup=0, dry_run=False):
    """Entry point for CLI to clean up hugepages"""

    # import needs to be done inside this big so that calls to `dragon-config`
    # via an alias don't lead to ModuleNotFound errors when we're doing a dev
    # build from scratch
    from dragon.utils import get_hugepage_mount

    hugepage_fs_mount = get_hugepage_mount()

    if not careful_restart_cleanup:
        try:
            careful_restart_cleanup = int(sys.argv[1])
        except (KeyError, IndexError, ValueError):
            careful_restart_cleanup = 0

    if hugepage_fs_mount is not None:
        print(f"cleaning up hugepage files in {hugepage_fs_mount}", flush=True)
        for file_name in os.listdir(hugepage_fs_mount):
            file_path = os.path.join(hugepage_fs_mount, file_name)
            if careful_restart_cleanup == 1:
                if "dict_pool" in file_name:
                    print(f"leaving {file_name}", flush=True)
                    continue

            if dry_run:
                print(f"DRY_RUN: deleting {file_path}", flush=True)
                continue

            try:
                print(f"deleting {file_path}", flush=True)
                os.unlink(file_path)
            except Exception:
                print(f"failed to delete {file_path} while cleaning up hugepage files", flush=True)
    else:
        print("no hugepage files to clean up", flush=True)


def print_compile_and_link_lines():
    """Print the compiler and link line options with brief description preamble"""

    print(f'Compile line options: "{print_compiler_options(print_line=False)}"\n')
    print(f'Link line options: "{print_linker_options(print_line=False)}"\n')


def print_compiler_options(print_line=True):
    """Print only the compiler include line options"""

    try:
        from ..infrastructure.facts import DRAGON_BASE_DIR
    except ImportError:
        raise RuntimeError(
            "Dragon environment is not installed or built. We're unable to provide header or library options as a result."
        )

    compiler_options = f"-I {os.path.abspath(os.path.join(DRAGON_BASE_DIR, 'include'))}"

    if print_line:
        print(f"{compiler_options}")
    else:
        return compiler_options


def print_linker_options(print_line=True):
    """Print only the linker link line options"""

    try:
        from ..infrastructure.facts import DRAGON_BASE_DIR
    except ImportError:
        raise RuntimeError(
            "Dragon environment is not installed or built. We're unable to provide linking options as a result."
        )

    link_line_options = f"-L {os.path.abspath(os.path.join(DRAGON_BASE_DIR, 'lib'))} -ldragon"
    if print_line:
        print(f"{link_line_options}")
    else:
        return link_line_options


def _configure_parser():
    """Configure the argparse parser for dragon-config"""

    parser = argparse.ArgumentParser(
        prog="dragon-config",
        formatter_class=RawTextHelpFormatter,
        description="""
    Configure the build and runtime environments for Dragon
    in regards to 3rd party libraries. This is needed for building
    network backends for HSTA, as well as for GPU support more generally.
    In future releases, this script may also be used for runtime
    configuration of libraries. Additionally, some options provide
    information about the Dragon installation to allow Dragon header files
    and libraries to be used in compiled applications""",
    )

    parser.add_argument("-c", "--clean", help="Clean out all config information.", action="store_true")
    parser.add_argument("-s", "--serialize", help=SERIALIZE_HELP, action="store_true")
    parser.add_argument("-g", "--get", help=GET_KV_HELP)

    subparser = parser.add_subparsers(
        title="Add and tests paths subparser",
        help="Add paths for configuration, compilation, execution, and testing of Dragon",
        dest="add",
    )
    add_subparser = subparser.add_parser("add", help=DRAGON_CONFIG_ADD_HELP, formatter_class=RawTextHelpFormatter)
    test_subparser = subparser.add_parser("test", help=DRAGON_CONFIG_TEST_HELP, formatter_class=RawTextHelpFormatter)

    def list_str(values):
        # Split at any colons to allow multiple paths
        return values.split(":")

    # Include options
    add_subparser.add_argument(
        "--ofi-include", type=list_str, help="Include path for OFI headers to be used when building dragon"
    )
    add_subparser.add_argument(
        "--ucx-include", type=list_str, help="Include path for UCX headers to be used when building dragon"
    )
    add_subparser.add_argument(
        "--pmix-include", type=list_str, help="Include path for PMIx headers to be used when building dragon"
    )
    add_subparser.add_argument(
        "--mpi-include", type=list_str, help="Include path for MPI headers to be used when building dragon"
    )
    add_subparser.add_argument(
        "--cuda-include", type=list_str, help="Include path for CUDA headers to be used when building dragon"
    )
    add_subparser.add_argument(
        "--hip-include", type=list_str, help="Include path for HIP headers to be used when building dragon"
    )
    add_subparser.add_argument(
        "--ze-include", type=list_str, help="Include path for Ze headers to be used when building dragon"
    )

    # Build lib options
    add_subparser.add_argument(
        "--ofi-build-lib",
        type=list_str,
        help="Path to OFI libraries (eg: libfabric.so) to be used when building dragon",
    )
    add_subparser.add_argument(
        "--ucx-build-lib", type=list_str, help="Path to UCX libraries (eg: libucp.so) to be used when building dragon"
    )

    # Runtime lib options
    add_subparser.add_argument(
        "--ofi-runtime-lib",
        type=list_str,
        help="Path to OFI libraries (eg: libfabric.so) to be used during app exeuction",
    )
    add_subparser.add_argument(
        "--ucx-runtime-lib", type=list_str, help="Path to UCX libraries (eg: libucp.so) to be used during app execution"
    )
    add_subparser.add_argument(
        "--cuda-runtime-lib",
        type=list_str,
        help="Path to CUDA libraries (eg: libcudart.so) to be used during app execution",
    )

    # Backend mpiexec override options
    add_subparser.add_argument("--netconfig-mpiexec-override", type=str, help=DRAGON_NETWORK_MPIEXEC_HELP)
    add_subparser.add_argument("--backend-mpiexec-override", type=str, help=DRAGON_BACKEND_MPIEXEC_HELP)

    add_subparser.add_argument(
        "--tcp-runtime",
        action="store_true",
        help="If only using TCP for backend communication, set in order to turn off warning message during initialization of runtime",
    )

    # Test subparsers
    test_subparser.add_argument("--cray-mpich", type=str, help="Path to Cray MPICH installation")
    test_subparser.add_argument("--open-mpi", type=str, help="Path to Open MPI installation")
    test_subparser.add_argument("--anl-mpich", type=str, help="Path to ANL MPICH installation")

    # Options for building dragon runtime
    compile_group = parser.add_mutually_exclusive_group()
    compile_group.add_argument("-l", "--linker-options", action="store_true", help=LINKER_HELP)
    compile_group.add_argument("-o", "--compiler-options", action="store_true", help=COMPILER_HELP)
    compile_group.add_argument("-e", "--explicit-compiler-options", action="store_true", help=EXPLICIT_COMPILE_HELP)

    return parser


def _handle_subparsers(args, base_dir, config_filename, makefile_filename):
    """Manages any things that is provides via the add subparser"""

    args_dict = vars(args)
    defined_keys = [k for k, v in args_dict.items() if v not in [None, False, True] and k in ADD_KEYS | TEST_KEYS]

    if len(defined_keys) != 0:
        if base_dir == "":
            print("failed to update environment: DRAGON_BASE_DIR not set, try hack/setup", flush=True)
            sys.exit()

        if config_filename == "" or makefile_filename == "":
            print("failed to update environment: unable to find environment file(s)", flush=True)

        # Put together the json struct we'll use through the runtime while executing
        if os.path.isfile(config_filename):
            with open(config_filename) as config_file:
                config_dict = json.load(config_file)
        else:
            config_dict = {}

        new_env = {k: args_dict[k] for k in defined_keys}
        config_dict.update(new_env)

        # Put together the makefile and json config files
        with open(makefile_filename, "w") as make_file:
            for key, paths in config_dict.items():
                # Sometimes, we've found we get to this point and somehow don't have the paths as a list and iterate
                # through chars. So make sure we have a proper list
                if not isinstance(paths, list):
                    paths = [paths]

                for dpath in paths:
                    path = Path(dpath)

                    # Some of the keys in the config dict may not have routings.
                    try:
                        routing_key = ROUTING_ENUM[key]
                        _ATBL[routing_key][0](make_file, config_filename, path, base_dir)
                    except KeyError:
                        pass


def dragon_config():
    try:
        from ..infrastructure.facts import CONFIG_FILE_PATH

        if CONFIG_FILE_PATH.exists():
            with open(CONFIG_FILE_PATH) as config_file:
                config_dict = json.load(config_file)

            return config_dict

    except Exception:
        pass

    return {}


def main():
    # get args
    parser = _configure_parser()
    args = parser.parse_args()

    # If link line options were requested, print them and exit
    if args.explicit_compiler_options:
        print_compile_and_link_lines()
        parser.exit()
    elif args.linker_options:
        print_linker_options()
        parser.exit()
    elif args.compiler_options:
        print_compiler_options()
        parser.exit()

    # set base dir and other paths. It may seem silly to have a complicated
    # try/except here, but we have to be able to run config before we build dragon
    # and you can't import facts.py until dragon is built. It's an annoying catch 22
    try:
        from ..infrastructure.facts import DRAGON_BASE_DIR, DRAGON_CONFIG_DIR

        # Try to use these if defined
        base_dir = DRAGON_BASE_DIR
        config_dir = DRAGON_CONFIG_DIR

    # If we end up here, we don't have dragon installed yet but need to defined
    # these paths so we CAN build and install dragon
    except ImportError:
        # This is the path used just about solely by the DST pipeline.
        # The other cases, we're able to work out where we're installed
        try:
            root_dir = Path(os.environ["DRAGON_BASE_DIR"])
            base_dir = root_dir / "dragon"
            config_dir = base_dir / ".hsta-config"
        except KeyError:
            base_dir = ""
            config_dir = ""

    try:
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)

        config_filename = f"{config_dir}/dragon-config.json"
        makefile_filename = f"{base_dir}/.dragon-config.mk"
    except Exception:
        config_filename = ""
        makefile_filename = ""

    # handle serialize command before updating anything
    if args.serialize:
        if config_filename == "":
            print("failed to serialize environment: unable to find environment file", flush=True)
            sys.exit()

        if os.path.isfile(config_filename):
            with open(config_filename) as config_file:
                config_dict = json.load(config_file)

            ser_config = ""
            the_first_one = True
            for key in config_dict:
                if the_first_one:
                    ser_config += f"{key}={config_dict[key]}"
                    the_first_one = False
                else:
                    ser_config += f":{key}={config_dict[key]}"
            print(ser_config, flush=True)
        else:
            print("no environment configuration available", flush=True)

    if args.get:
        if config_filename == "":
            print("", flush=True)

        if os.path.isfile(config_filename):
            with open(config_filename) as config_file:
                config_dict = json.load(config_file)

            try:
                ser_config = f"{config_dict[args.get]}"
                print(ser_config, flush=True)
            except KeyError:
                print("", flush=True)
        else:
            print("", flush=True)

    # handle 'clean' command (do this first, so clean+set acts as a reset)
    if args.clean:
        if config_filename == "" or makefile_filename == "":
            print("failed to clean environment: unable to find environment file(s)", flush=True)
            sys.exit()

        try:
            os.remove(config_filename)
            os.remove(makefile_filename)
        except Exception:
            pass

    # handle the slew of 'add' and test subparsers
    _handle_subparsers(args, base_dir, config_filename, makefile_filename)
