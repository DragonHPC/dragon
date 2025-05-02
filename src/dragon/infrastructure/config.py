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

DRAGON_CONFIG_ADD_HELP = """Add a colon-separated list of key-value pairs (key=value) to configure
include and library paths for Dragon, or to make the TCP runtime the always-on
default for backend communication (set to True). Possible keys: tcp-runtime,
ofi-[build,runtime]-lib, ofi-include, ucx-[build,runtime]-lib, ucx-include,
mpi-[build,runtime]-lib, mpi-include, cuda-include, hip-include, ze-include.

Examples
--------
UCX backend:
dragon-config -a 'ucx-include=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/include/'
dragon-config -a 'ucx-build-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib'
dragon-config -a 'ucx-runtime-lib=/opt/nvidia/hpc_sdk/Linux_x86_64/23.11/comm_libs/12.3/hpcx/hpcx-2.16/ucx/prof/lib'

Set TCP transport as always-on default backend:
dragon-config -a 'tcp-runtime=True'

"""

SERIALIZE_HELP = """Serialize all key-value pairs currently in the configuration file into a single,
colon-separated string that can be passed to the --add command.
"""


def remove_suffix(string_w_suffix, suffix):
    if suffix and string_w_suffix.endswith(suffix):
        return string_w_suffix[: -len(suffix)]
    else:
        return string_w_suffix


def hugepages_cleanup():
    """Entry point for CLI to clean up hugepages"""

    # import needs to be done inside this big so that calls to `dragon-config`
    # via an alias don't lead to ModuleNotFound errors when we're doing a dev
    # build from scratch
    from dragon.utils import get_hugepage_mount

    hugepage_fs_mount = get_hugepage_mount()

    try:
        careful_restart_cleanup = int(sys.argv[1])
    except IndexError:
        careful_restart_cleanup = 0

    if hugepage_fs_mount is not None:
        print(f"cleaning up hugepage files in {hugepage_fs_mount}")
        for file_name in os.listdir(hugepage_fs_mount):
            file_path = os.path.join(hugepage_fs_mount, file_name)
            if careful_restart_cleanup == 1:
                if "dict_pool" in file_name:
                    print(f"leaving {file_name}")
                    continue
            try:
                os.unlink(file_path)
            except Exception:
                print(f"failed to delete {file_path} while cleaning up hugepage files", flush=True)
    else:
        print("no hugepage files to clean up")


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

    parser.add_argument("-a", "--add", help=DRAGON_CONFIG_ADD_HELP)
    parser.add_argument("-c", "--clean", help="Clean out all config information.", action="store_true")
    parser.add_argument("-s", "--serialize", help=SERIALIZE_HELP, action="store_true")

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

        base_dir = DRAGON_BASE_DIR
        config_dir = DRAGON_CONFIG_DIR
    except ImportError:
        try:
            base_dir = Path(os.environ["DRAGON_BASE_DIR"])
            config_dir = base_dir / "dragon" / ".hsta-config"
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

    # handle 'add' command
    if args.add is not None:
        if base_dir == "":
            print("failed to update environment: DRAGON_BASE_DIR not set, try hack/setup", flush=True)
            sys.exit()

        if config_filename == "" or makefile_filename == "":
            print("failed to update environment: unable to find environment file(s)", flush=True)

        if os.path.isfile(config_filename):
            with open(config_filename) as config_file:
                config_dict = json.load(config_file)
        else:
            config_dict = {}

        user_input = args.add.split(":")
        new_env = dict(kv.split("=", 1) for kv in user_input)
        config_dict.update(new_env)

        with open(config_filename, "w") as config_file:
            json.dump(config_dict, config_file)

        with open(makefile_filename, "w") as make_file:
            for key in config_dict:
                path = Path(config_dict[key])
                if "build-lib" in key:
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
                    make_file.write(f"CONFIG_BACKEND_LIB_DEPS := $(CONFIG_BACKEND_LIB_DEPS) lib{backend_libname}.so\n")

                if "runtime-lib" in key:
                    libname, backend_libname = _get_libname(key)
                    path = Path(config_dict[key])

                    # sanity check
                    lib = path / f"lib{libname}.so"
                    if not os.path.isfile(lib):
                        print(f"{lib} does not exist, make sure file paths have been set correctly", flush=True)

                if "include" in key:
                    path = Path(config_dict[key])
                    make_file.write(f"CONFIG_INCLUDE := $(CONFIG_INCLUDE) -I{path}\n")

                    if "cuda" in key:
                        # sanity check
                        header = path / "cuda_runtime_api.h"
                        if not os.path.isfile(header):
                            print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/cuda.cpp\n")
                        make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_CUDA_INCLUDE\n")

                    if "hip" in key:
                        # sanity check
                        header = path / "hip" / "hip_runtime_api.h"
                        if not os.path.isfile(header):
                            print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/hip.cpp\n")
                        make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_HIP_INCLUDE\n")

                    if "ze" in key:
                        # sanity check
                        header = path / "ze_api.h"
                        if not os.path.isfile(header):
                            print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_SOURCES := $(CONFIG_SOURCES) {base_dir}/lib/gpu/ze.cpp\n")
                        make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_ZE_INCLUDE\n")

                    if "ofi" in key:
                        # sanity check
                        header = path / "rdma" / "fabric.h"
                        if not os.path.isfile(header):
                            print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_OFI_INCLUDE\n")

                    if "ucx" in key:
                        # sanity check
                        header = path / "ucp" / "api" / "ucp.h"
                        if not os.path.isfile(header):
                            print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_UCX_INCLUDE\n")

                    if "mpi" in key:
                        # sanity check
                        header = path / "mpi.h"
                        if not os.path.isfile(header):
                            print(f"{header} does not exist, make sure file paths have been set correctly", flush=True)

                        make_file.write(f"CONFIG_DEFINES := $(CONFIG_DEFINES) -DHAVE_MPI_INCLUDE\n")
