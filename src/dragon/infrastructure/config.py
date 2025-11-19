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
dragon-config add --tcp-runtime

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
single string. The following special strings are necessary and will be automatically filled in at
the time of use by Dragon:

{nodes} = number of nodes, {nodelist} = comma separated list of nodes

Examples
--------
Set launcher mpiexec backend launch override for Cray-PALS:
$ dragon-config add --backend-mpiexec-override='mpiexec --np {nnodes} --ppn 1 --cpu-bind none --hosts {nodelist} --line-buffer'

Set launcher mpiexec backend launch override for OpenMPI 5.0.6:
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


def _load_config_from_file(config_filename):
    """Try loading json config into dict. If empty, return empty ddict."""

    if os.path.isfile(config_filename):
        with open(config_filename) as config_file:
            try:
                config_dict = json.load(config_file)
            # If the file is empty or malformed, start fresh
            except json.decoder.JSONDecodeError:
                config_dict = {}
    else:
        config_dict = {}

    return config_dict


def _update_json_config(config_filename, key, val):
    """Update the json config file loaded during runtime execution"""

    config_dict = _load_config_from_file(config_filename)

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


def _concat_search_files(base_dir, routing_option):
    """Single place to find all the files we check the existence of for a given routing option"""

    search_files = []

    if routing_option == ROUTING_ENUM.ofi_include:
        search_files = [Path("rdma") / "fabric.h"]
    elif routing_option == ROUTING_ENUM.ucx_include:
        search_files = [Path("ucp") / "api" / "ucp.h"]
    elif routing_option == ROUTING_ENUM.cuda_include:
        search_files = [Path("cuda_runtime_api.h")]
    elif routing_option == ROUTING_ENUM.hip_include:
        search_files = [Path("hip") / "hip_runtime_api.h"]
    elif routing_option == ROUTING_ENUM.ze_include:
        search_files = [Path("ze_api.h")]
    elif routing_option == ROUTING_ENUM.mpi_include:
        search_files = [Path("mpi.h")]
    elif routing_option == ROUTING_ENUM.pmix_include:
        search_files = [Path("src/include/pmix_globals.h"), Path("pmix_common.h"), Path("event.h")]
    elif routing_option == ROUTING_ENUM.ofi_build_lib:
        search_files = [Path("libfabric.so")]
    elif routing_option == ROUTING_ENUM.ucx_build_lib:
        search_files = [Path("libucp.so")]
    elif routing_option == ROUTING_ENUM.anl_mpich:
        search_files = [Path("lib/libmpich.so"), Path("lib64/libmpich.so"), Path("include/mpi.h")]
    elif routing_option == ROUTING_ENUM.cray_mpich:
        search_files = [Path("lib/libmpich.so"), Path("lib64/libmpich.so"), Path("include/mpi.h")]
    elif routing_option == ROUTING_ENUM.open_mpi:
        search_files = [Path("lib/libmpi.so"), Path("lib64/libmpi.so"), Path("include/mpi.h")]

    full_search_paths = []
    if len(search_files) != 0:
        full_search_paths = [Path(base_dir) / path for path in search_files]
    # else:
    #    full_search_paths = [Path(base_dir)]

    return full_search_paths


def _check_filepaths_exist(path, routing_option):
    """Check that the expected files for a given routing option exist in the path"""
    search_files = _concat_search_files(path, routing_option)
    if all(not os.path.isfile(h) for h in search_files):
        print(
            f"{', '.join([str(p) for p in search_files])} do(es) not exist. Make sure file paths have been set correctly",
            flush=True,
        )
        sys.exit(1)
    return search_files


def _check_add_filepaths_exist(path, routing_option):
    """Check that the expected files for a given add option exist in the path"""
    return _check_filepaths_exist(path, routing_option)


def _check_test_filepaths_exist(path, routing_option):
    """Check that the expected files for a given routing option exist in the path"""

    search_files = _check_filepaths_exist(path, routing_option)

    # Cycle through the paths and get the lib and include directories that actually exist
    lib_path = None
    include_path = None
    for f in search_files:
        if os.path.isfile(f):
            full_dir = os.path.dirname(f)
            if "lib" in os.path.basename(full_dir):
                lib_path = full_dir
            elif "include" in os.path.basename(full_dir):
                include_path = full_dir

    return lib_path, include_path


@config_router(ROUTING_ENUM.ofi_include, _ATBL)
def _ofi_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.ofi_include)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_OFI_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.ofi_include.name, str(path))


@config_router(ROUTING_ENUM.ucx_include, _ATBL)
def _ucx_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.ucx_include)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_UCX_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.ucx_include.name, str(path))


@config_router(ROUTING_ENUM.cuda_include, _ATBL)
def _cuda_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.cuda_include)

    make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/cuda.cpp\n")
    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_CUDA_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.cuda_include.name, str(path))


@config_router(ROUTING_ENUM.hip_include, _ATBL)
def _hip_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.hip_include)

    make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/hip.cpp\n")
    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_HIP_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.hip_include.name, str(path))


@config_router(ROUTING_ENUM.ze_include, _ATBL)
def _ze_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.ze_include)

    make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/ze.cpp\n")
    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_ZE_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.ze_include.name, str(path))


@config_router(ROUTING_ENUM.mpi_include, _ATBL)
def _mpi_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.mpi_include)

    make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_MPI_INCLUDE\n")
    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

    _update_json_config(config_filename, ROUTING_ENUM.mpi_include.name, str(path))


@config_router(ROUTING_ENUM.pmix_include, _ATBL)
def _pmix_include_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.pmix_include)

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


@config_router(ROUTING_ENUM.tcp_runtime, _ATBL)
def _tcp_runtime_lib(make_file, config_filename, path, base_dir):
    _update_json_config(config_filename, ROUTING_ENUM.tcp_runtime.name, path)


@config_router(ROUTING_ENUM.netconfig_mpiexec_override, _ATBL)
def _netconfig_mpiexec_override(make_file, config_filename, path, base_dir):
    _update_json_config(config_filename, ROUTING_ENUM.netconfig_mpiexec_override.name, path)


@config_router(ROUTING_ENUM.backend_mpiexec_override, _ATBL)
def _backend_mpiexec_override(make_file, config_filename, path, base_dir):
    _update_json_config(config_filename, ROUTING_ENUM.backend_mpiexec_override.name, path)


@config_router(ROUTING_ENUM.ofi_build_lib, _ATBL)
def _ofi_build_lib_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.ofi_build_lib)

    make_file.write("CONFIG_OFI_LIBS := \n")
    make_file.write("CONFIG_BACKEND_LIBS := $(CONFIG_BACKEND_LIBS) -L. -lfabric\n")
    make_file.write("CONFIG_BACKEND_LIB_DEPS := $(CONFIG_BACKEND_LIB_DEPS) libdfabric_ofi.so\n")

    _update_json_config(config_filename, ROUTING_ENUM.ofi_build_lib.name, str(path))


@config_router(ROUTING_ENUM.ucx_build_lib, _ATBL)
def _ucx_build_lib_route(make_file, config_filename, path, base_dir):
    # sanity check
    _check_add_filepaths_exist(path, ROUTING_ENUM.ucx_build_lib)

    make_file.write("CONFIG_UCX_LIBS := \n")
    make_file.write("CONFIG_BACKEND_LIBS := $(CONFIG_BACKEND_LIBS) -L. -lucp\n")
    make_file.write("CONFIG_BACKEND_LIB_DEPS := $(CONFIG_BACKEND_LIB_DEPS) libdfabric_ucx.so\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.ucx_build_lib.name, str(path))


@config_router(ROUTING_ENUM.cray_mpich, _ATBL)
def _cray_mpich_test_route(make_file, config_filename, path, base_dir):
    # Get our include and lib paths:
    libname = "mpich"
    lib_path, include_path = _check_test_filepaths_exist(path, ROUTING_ENUM.cray_mpich)

    # Make sure they weren't left empty:
    if lib_path is None or include_path is None:
        print(
            "Could not find both lib and include paths in input Cray MPICH path, make sure file path has been set correctly",
            flush=True,
        )
        sys.exit(1)

    make_file.write(f"TEST_CRAY_MPICH_FLAGS := -I {include_path} -L {lib_path} -l{libname}\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.cray_mpich.name, str(path))
    _update_json_config(config_filename, ROUTING_ENUM.cray_mpich.name + "_runtime_lib", str(lib_path))


@config_router(ROUTING_ENUM.open_mpi, _ATBL)
def _open_mpi_test_route(make_file, config_filename, path, base_dir):
    # Get our include and lib paths:
    libname = "mpi"
    lib_path, include_path = _check_test_filepaths_exist(path, ROUTING_ENUM.open_mpi)

    # Make sure they weren't left empty:
    if lib_path is None or include_path is None:
        print(
            "Could not find both lib and include paths in input Open MPI path, make sure file path has been set correctly",
            flush=True,
        )
        sys.exit(1)

    make_file.write(f"TEST_OPEN_MPI_FLAGS := -I {include_path} -L {lib_path} -l{libname}\n")

    # Make sure the runtime lib is in the json config file
    _update_json_config(config_filename, ROUTING_ENUM.open_mpi.name, str(path))
    _update_json_config(config_filename, ROUTING_ENUM.open_mpi.name + "_runtime_lib", str(lib_path))


@config_router(ROUTING_ENUM.anl_mpich, _ATBL)
def _anl_mpich_test_route(make_file, config_filename, path, base_dir):
    # Get our include and lib paths:
    libname = "mpich"
    lib_path, include_path = _check_test_filepaths_exist(path, ROUTING_ENUM.anl_mpich)

    # Make sure they weren't left empty:
    if lib_path is None or include_path is None:
        print(
            "Could not find both lib and include paths in input ANL MPICH path, make sure file path has been set correctly",
            flush=True,
        )
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
    parser.add_argument(
        "--config-file", help="Point configuration to a custom config file. Largely intended for testing"
    )
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
    defined_keys = [k for k, v in args_dict.items() if v is not None and k in ADD_KEYS | TEST_KEYS]

    if len(defined_keys) != 0:
        if base_dir == "":
            print("failed to update environment: DRAGON_BASE_DIR not set, try hack/setup", flush=True)
            sys.exit()

        if config_filename == "" or makefile_filename == "":
            print("failed to update environment: unable to find environment file(s)", flush=True)

        # Put together the json struct we'll use through the runtime while executing
        config_dict = _load_config_from_file(config_filename)
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
                    # Some of the keys in the config dict may not have routings.
                    try:
                        routing_key = ROUTING_ENUM[key]
                        _ATBL[routing_key][0](make_file, config_filename, dpath, base_dir)
                    except KeyError:
                        pass


def dragon_config():
    try:
        from ..infrastructure.facts import CONFIG_FILE_PATH

        config_dict = _load_config_from_file(CONFIG_FILE_PATH)
    except Exception:
        pass

    return config_dict


def _get_config_filenames():
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

    return makefile_filename, config_filename, base_dir, config_dir


def _serialize_config(config_filename):
    """Serialize all key-value pairs currently in the configuration file"""

    if config_filename == "":
        print("failed to serialize environment: unable to find environment file", flush=True)
        sys.exit()

    config_dict = _load_config_from_file(config_filename)

    if len(config_dict) != 0:
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


def _get_config(config_filename, key):
    """Get value for given key from config file and print to stdout"""
    if config_filename == "":
        print("", flush=True)

    config_dict = _load_config_from_file(config_filename)
    if len(config_dict) != 0:
        try:
            ser_config = f"{config_dict[key]}"
            print(ser_config, flush=True)
        except KeyError:
            print("", flush=True)
    else:
        print("", flush=True)


def _clean_config(config_filename, makefile_filename):
    """Remove config files from disk"""
    if config_filename == "" or makefile_filename == "":
        print("failed to clean environment: unable to find environment file(s)", flush=True)
        sys.exit()

    try:
        os.remove(config_filename)
        os.remove(makefile_filename)
    except Exception:
        pass


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

    # Get filenames
    makefile_filename, config_filename, base_dir, config_dir = _get_config_filenames()

    # If we were given a config file on the command line, override it. This should really only be used for testing.
    if args.config_file:
        config_filename = args.config_file

    # handle serialize command before updating anything
    if args.serialize:
        _serialize_config(config_filename)

    # Do just a single get
    if args.get:
        _get_config(config_filename, args.get)

    # handle 'clean' command (do this first, so clean+set acts as a reset)
    if args.clean:
        _clean_config(config_filename, makefile_filename)

    # handle the slew of 'add' and test subparsers
    _handle_subparsers(args, base_dir, config_filename, makefile_filename)
