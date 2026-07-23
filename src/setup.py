import os
from functools import partial
from pathlib import Path
import re
from collections import defaultdict
import platform
import shutil
from subprocess import check_call

from setuptools import Extension, find_packages, setup
from setuptools.command.build import build
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.editable_wheel import editable_wheel

from Cython.Build import cythonize

from dragon.cli import entry_points

ROOTDIR = str(Path(__file__).parent)
LIBRARIES = ["dragon"]


def get_extra_requires(path, add_all=True):
    with open(path) as fp:
        extra_deps = defaultdict(set)
        for k in fp:
            if k.strip() and not k.startswith("#"):
                tags = set()
                if ":" in k:
                    k, v = k.split(":")
                    tags.update(vv.strip() for vv in v.split(","))
                tags.add(re.split("[<=>]", k)[0])
                for t in tags:
                    extra_deps[t].add(k)

        # add tag `all` at the end
        if add_all:
            extra_deps["all"] = set(vv for v in extra_deps.values() for vv in v)

    return extra_deps


def make_relative_rpath_args(path):
    """Construct platform-appropriate RPATH to support binary
    wheels that ship with libdragon.so, etc."""
    if platform.system() == "Darwin":
        return [f"-Wl,-rpath,@loader_path/{path}"]

    # Linux/ELF: $ORIGIN resolves to the loaded extension module's directory,
    # analogous to @loader_path on macOS, so wheels remain relocatable.
    return [f"-Wl,-rpath,$ORIGIN/{path}"]


DragonExtension = partial(
    Extension,
    include_dirs=[
        f"{ROOTDIR}/lib",
        f"{ROOTDIR}/include",
        f"{ROOTDIR}/lib/gpu",
        f"{ROOTDIR}/lib/pmix",
        f"{ROOTDIR}/lib/event",
    ],
    library_dirs=[f"{ROOTDIR}/lib"],
    libraries=LIBRARIES,
    extra_link_args=make_relative_rpath_args("lib"),
)


extensions = [
    DragonExtension("dragon.utils", ["dragon/pydragon_utils.pyx"]),
    DragonExtension("dragon.rc", ["dragon/pydragon_rc.pyx"]),
    DragonExtension("dragon.dtypes", ["dragon/pydragon_dtypes.pyx"]),
    DragonExtension("dragon.pheap", ["dragon/pydragon_heap.pyx"]),
    DragonExtension("dragon.locks", ["dragon/pydragon_lock.pyx"]),
    DragonExtension("dragon.channels", ["dragon/pydragon_channels.pyx"]),
    DragonExtension("dragon.managed_memory", ["dragon/pydragon_managed_memory.pyx"]),
    DragonExtension("dragon.dlogging.logger", ["dragon/dlogging/pydragon_logging.pyx"]),
    DragonExtension("dragon.pmod", ["dragon/pydragon_pmod.pyx"]),
    DragonExtension("dragon.perf", ["dragon/pydragon_perf.pyx"]),
    DragonExtension("dragon.fli", ["dragon/pydragon_fli.pyx"]),
    DragonExtension("dragon.dpmix", ["dragon/pydragon_dpmix.pyx"]),
]


def stage_package_dependencies(rootdir: Path):
    """Stage wheel assets inside dragon/ so setuptools includes them."""

    # Include binaries and libraries created by make
    lib_tempdir = rootdir / "dragon" / "lib"
    try:
        # Files included in wheels must appear under the package tree.
        lib_tempdir.symlink_to(rootdir / "lib")
    except:
        if not lib_tempdir.is_symlink():
            raise

    bin_tempdir = rootdir / "dragon" / "bin"
    try:
        bin_tempdir.symlink_to(rootdir / "bin")
    except:
        if not bin_tempdir.is_symlink():
            raise

    # Get the capnp message defs in the wheel
    message_defs_file = rootdir / "dragon" / "infrastructure" / "message_defs.capnp"
    try:
        message_defs_file.unlink(missing_ok=True)  # needed if rebuilding w/different dir structure.
        message_defs_file.symlink_to(rootdir / "lib" / "message_defs.capnp")
    except:
        if not message_defs_file.is_symlink():
            raise

    # Make sure the capnp include directories get in the correct place
    # Package up the include subdirectories
    include_tempdir = rootdir / "dragon" / "include"
    include_tempdir.mkdir(parents=True, exist_ok=True)
    for subdir in ("dragon", "kj", "capnp"):
        src = rootdir / "include" / subdir
        dst = include_tempdir / subdir
        if dst.is_symlink() or dst.is_file():
            dst.unlink()
        elif dst.is_dir():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)


def run_make_build(rootdir: Path):
    make_cmd = ["make", "build"]
    check_call(make_cmd, cwd=rootdir)


class DragonBuild(build):
    def run(self):
        rootdir = Path(ROOTDIR)

        def compile():
            run_make_build(rootdir)

        self.execute(compile, [], "Compiling Cython dependencies with make")
        super().run()


class DragonEditableWheel(editable_wheel):
    def run(self):
        rootdir = Path(ROOTDIR)

        def compile():
            run_make_build(rootdir)

        self.execute(compile, [], "Compiling Cython dependencies with make")
        super().run()


class DragonBuild_ext(build_ext):
    user_options = build.user_options + [("cythonize", None, "cythonize packages and modules")]

    boolean_options = build.boolean_options + ["cythonize"]

    def initialize_options(self):
        super().initialize_options()
        self.cythonize = 0

    def run(self):
        _cythonize = partial(
            cythonize,
            nthreads=int(
                os.environ.get("DRAGON_BUILD_NTHREADS", os.cpu_count() if platform.system() != "Darwin" else 0)
            ),
            show_all_warnings=True,
            compiler_directives={"language_level": "3", "boundscheck": False},
            build_dir=self.build_temp,
        )

        # Cythonize core extensions
        self.distribution.ext_modules = _cythonize(self.distribution.ext_modules, force=bool(self.cythonize))

        if self.cythonize:
            # Cythonize everything except dragon.cli and dragon.__main__
            self.distribution.packages = ["dragon.cli"]
            self.distribution.py_modules = ["dragon.__main__"]
            self.distribution.ext_modules.extend(
                _cythonize(
                    "dragon/**/*.py",
                    exclude=["dragon/**/setup.py", "dragon/cli/*.py", "dragon/__main__.py"],
                    compiler_directives={
                        "language_level": "3",
                        # Disable annotation typing when compiling arbitrary
                        # Python 3.x code.
                        "annotation_typing": False,
                        "boundscheck": False,
                    },
                    force=True,
                )
            )

            build_ext_options = self.distribution.command_options.setdefault("build_ext", {})
            build_ext_options["inplace"] = ("setup script", 0)

        super().run()


class DragonBuild_py(build_py):
    # The find_modules() implementation in setuptools (really distutils)
    # automatically adds __init__.py for any non-root package module listed in
    # py_modules. As a result, our implementation is a simplified variant of
    # the original that does not automatically add __init__.py.

    def run(self):
        stage_package_dependencies(Path(ROOTDIR))

        # Include all staged header files recursively in package data.
        # We can't add it just via package_data in setup() because setuptools
        # doesn't support a recursive glob, and we want to avoid hardcoding the list
        # of header files.
        dragon_root = Path(ROOTDIR) / "dragon"
        include_root = dragon_root / "include"
        package_data = self.distribution.package_data.setdefault("dragon", [])
        include_files = [p.relative_to(dragon_root).as_posix() for p in include_root.rglob("*") if p.is_file()]
        for rel_path in include_files:
            if rel_path not in package_data:
                package_data.append(rel_path)

        super().run()

    def find_modules(self):
        """Finds individually-specified Python modules, ie. those listed by
        module name in 'self.py_modules'.  Returns a list of tuples (package,
        module_base, filename): 'package' is a tuple of the path through
        package-space to the module; 'module_base' is the bare (no
        packages, no dots) module name, and 'filename' is the path to the
        ".py" file (relative to the distribution root) that implements the
        module.
        """
        # Maps package names to the corresponding directory
        packages = {}

        # List of (package, module, filename) tuples to return
        modules = []

        for module in self.py_modules:
            path = module.split(".")
            package = ".".join(path[0:-1])
            module_base = path[-1]

            try:
                package_dir = packages[package]
            except KeyError:
                package_dir = self.get_package_dir(package)
                # We explicitly do not want to automatically add __init__.py,
                # hence we pass None as the first argument, but we still want
                # to check the package_dir in order to properly resolve the
                # module_file below.
                self.check_package(None, package_dir)
                packages[package] = package_dir

            module_file = os.path.join(package_dir, module_base + ".py")
            if not self.check_module(module, module_file):
                continue

            modules.append((package, module_base, module_file))

        return modules


if __name__ == "__main__":
    setup(
        cmdclass={
            "build": DragonBuild,
            "build_ext": DragonBuild_ext,
            "build_py": DragonBuild_py,
            "editable_wheel": DragonEditableWheel,
        },
        options={"build_py": {"compile": 1}, "build_ext": {"inplace": 1}},
        name="dragonhpc",
        version=os.environ.get("DRAGON_VERSION", "latest"),
        include_package_data=False,
        packages=find_packages(include=["dragon*"]),
        package_data={
            "dragon": [
                "lib/libdragon.so",
                "bin/dragon-*",
                "localservices/dragon-popen",
                "infrastructure/message_defs.capnp",
            ]
        },
        ext_modules=extensions,
        entry_points=entry_points,
        install_requires=[
            "cloudpickle>=3.0.0",
            "pyyaml>=6.0.2",
            "psutil>=5.9.0",
            "pycapnp>=2.0.0,<2.2.0",
            "paramiko>=3.5.1",
            "shtab>=1.6.0",
        ],
        extras_require=get_extra_requires("extra-requirements.txt"),
    )
