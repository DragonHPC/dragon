import argparse
from argparse import RawTextHelpFormatter
import json
import os
import sys
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


def remove_suffix(string_w_suffix, suffix):
    if suffix and string_w_suffix.endswith(suffix):
        return string_w_suffix[: -len(suffix)]
    else:
        return string_w_suffix


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


def _get_libname(key):
    """Given a key from the dragon-config inputs, determine the requested backend

    :param key: a key accepted by `dragon-config -a`
    :type key: str
    :returns: libname for compiling, dfabric libname
    :rtype: (str, str)
    """
    if "ofi" in key:
        libname = "fabric"
        backend_libname = "dfabric_ofi"
    elif "ucx" in key:
        libname = "ucp"
        backend_libname = "dfabric_ucx"
    elif "mpi" in key:
        libname = "mpi"
        backend_libname = "dfabric_mpi"
    elif "pmi" in key:
        libname = "pmi2"
        backend_libname = ""
    elif "tcp" in key:
        libname = "tcp"
        backend_libname = ""

    return libname, backend_libname


def hsta_config():
    # get args
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
        title="Add paths subparser",
        help="Add paths for configuration, compilation, and execution of Dragon",
        dest="add",
    )
    add_subparser = subparser.add_parser("add", help=DRAGON_CONFIG_ADD_HELP, formatter_class=RawTextHelpFormatter)

    # Define all the keys for add entries:
    add_keys = {
        "ofi_include",
        "ucx_include",
        "pmix_include",
        "mpi_include",
        "cuda_include",
        "hip_include",
        "ze_include",
        "ofi_build_lib",
        "ucx_build_lib",
        "ofi_runtime_lib",
        "ucx_runtime_lib",
        "tcp_runtime",
        "netconfig_mpiexec_override",
        "backend_mpiexec_override",
    }

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

    add_subparser.add_argument("--netconfig-mpiexec-override", type=str, help=DRAGON_NETWORK_MPIEXEC_HELP)
    add_subparser.add_argument("--backend-mpiexec-override", type=str, help=DRAGON_BACKEND_MPIEXEC_HELP)

    add_subparser.add_argument(
        "--tcp-runtime",
        action="store_true",
        help="If only using TCP for backend communication, set in order to turn off warning message during initialization of runtime",
    )

    compile_group = parser.add_mutually_exclusive_group()
    compile_group.add_argument("-l", "--linker-options", action="store_true", help=LINKER_HELP)
    compile_group.add_argument("-o", "--compiler-options", action="store_true", help=COMPILER_HELP)
    compile_group.add_argument("-e", "--explicit-compiler-options", action="store_true", help=EXPLICIT_COMPILE_HELP)

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

    # handle the slew of 'add' commands
    # Convert the args Namespace into a dict and see if any of the keys in it are not None or from the tcp-runtime bool vals
    args_dict = vars(args)
    defined_adds = [k for k, v in args_dict.items() if v not in [None, False, True] and k in add_keys]

    if len(defined_adds) != 0:
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

        new_env = {k: args_dict[k] for k in defined_adds}
        config_dict.update(new_env)

        with open(config_filename, "w") as config_file:
            json.dump(config_dict, config_file)

        # Put together the makefile defines so we build dragon as requested
        with open(makefile_filename, "w") as make_file:
            for key, paths in config_dict.items():
                # Sometimes, we've found we get to this point and somehow don't have the paths as a list and iterate
                # through chars. So make sure we have a proper list
                if not isinstance(paths, list):
                    paths = [paths]
                for dpath in paths:
                    path = Path(dpath)
                    if "build_lib" in key:
                        libname, backend_libname = _get_libname(key)
                        if "ofi" in key:
                            make_file.write("CONFIG_OFI_LIBS := \n")
                        elif "ucx" in key:
                            make_file.write("CONFIG_UCX_LIBS := \n")

                        # sanity check
                        lib = path / f"lib{libname}.so"
                        if not os.path.isfile(lib):
                            print(f"{lib} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_BACKEND_LIBS := $(CONFIG_BACKEND_LIBS) -L. -l{backend_libname}\n")
                        make_file.write(
                            f"CONFIG_BACKEND_LIB_DEPS := $(CONFIG_BACKEND_LIB_DEPS) lib{backend_libname}.so\n"
                        )

                    if "runtime_lib" in key:
                        libname, backend_libname = _get_libname(key)

                        # sanity check
                        lib = path / f"lib{libname}.so"
                        if not os.path.isfile(lib):
                            print(f"{lib} does not exist, make sure file paths have been set correctly", flush=True)

                    if "include" in key:
                        make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

                        if "cuda" in key:
                            # sanity check
                            header = path / "cuda_runtime_api.h"
                            if not os.path.isfile(header):
                                print(
                                    f"{header} does not exist, make sure file paths have been set correctly", flush=True
                                )

                            make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/cuda.cpp\n")
                            make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_CUDA_INCLUDE\n")

                        if "hip" in key:
                            # sanity check
                            header = path / "hip" / "hip_runtime_api.h"
                            if not os.path.isfile(header):
                                print(
                                    f"{header} does not exist, make sure file paths have been set correctly", flush=True
                                )

                            make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/hip.cpp\n")
                            make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_HIP_INCLUDE\n")

                        if "ze" in key:
                            # sanity check
                            header = path / "ze_api.h"
                            if not os.path.isfile(header):
                                print(
                                    f"{header} does not exist, make sure file paths have been set correctly", flush=True
                                )

                            make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/ze.cpp\n")
                            make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_ZE_INCLUDE\n")

                        if "ofi" in key:
                            # sanity check
                            header = path / "rdma" / "fabric.h"
                            if not os.path.isfile(header):
                                print(
                                    f"{header} does not exist, make sure file paths have been set correctly", flush=True
                                )

                            make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_OFI_INCLUDE\n")

                        if "ucx" in key:
                            # sanity check
                            header = path / "ucp" / "api" / "ucp.h"
                            if not os.path.isfile(header):
                                print(
                                    f"{header} does not exist, make sure file paths have been set correctly", flush=True
                                )

                            make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_UCX_INCLUDE\n")

                        if "mpi" in key:
                            # sanity check
                            header = path / "mpi.h"
                            if not os.path.isfile(header):
                                print(
                                    f"{header} does not exist, make sure file paths have been set correctly", flush=True
                                )

                            make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_MPI_INCLUDE\n")

                        if "pmix" in key:
                            # sanity check
                            global_header = path / "src/include/pmix_globals.h"
                            common_header = path / "pmix_common.h"
                            event_header = path / "event.h"
                            if (
                                not os.path.isfile(global_header)
                                and not os.path.isfile(common_header)
                                and not os.path.isfile(event_header)
                            ):
                                print(
                                    f"{global_header}, {common_header}, nor {event_header} exist, make sure file paths have been set correctly",
                                    flush=True,
                                )

                            make_file.write("CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_PMIX_INCLUDE\n")


def dragon_config():
    try:
        from ..infrastructure.facts import CONFIG_FILE_PATH

        if CONFIG_FILE_PATH.exists():
            with open(CONFIG_FILE_PATH) as config_file:
                config_dict = json.load(config_file)

            return config_dict

    except:
        pass

    return {}
