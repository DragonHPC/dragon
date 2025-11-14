import os
from functools import partial
from pathlib import Path
import platform
from sysconfig import get_config_var

from setuptools import Extension, find_packages, setup
from setuptools.command.build import build as _build
from setuptools.command.build_py import build_py as _build_py
from Cython.Build import cythonize

from dragon.cli import entry_points


ROOTDIR = str(Path(__file__).parent)
LIBRARIES = ["dragon"] + ["rt"] if platform.system() != "Darwin" else []

def make_relative_rpath_args(path):
    """Construct platform-appropriate RPATH to support binary
    wheels that ship with libdragon.so, etc."""
    return [f"-Wl,-rpath,{ROOTDIR}/{path}"]


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


class build(_build):
    user_options = _build.user_options + [("cythonize", None, "cythonize packages and modules")]

    boolean_options = _build.boolean_options + ["cythonize"]

    def initialize_options(self):
        super().initialize_options()
        self.cythonize = 0

    def run(self):
        rootdir = Path(ROOTDIR)
        lib_tempdir = rootdir / "dragon" / "lib"
        try:
            # In order for setuptools to include files in a wheel, those
            # files must be in f'{ROOTDIR}/dragon' or a subdirectory; we
            # create a temporary symlink to point at f'{ROOTDIR}/lib' to
            # include 'libdragon.so' etc. in the binary wheel.
            lib_tempdir.symlink_to(rootdir / "lib")
        except:
            if not lib_tempdir.is_symlink():
                raise

        message_defs_file = rootdir / "dragon" / "infrastructure" / "message_defs.capnp"
        try:
            message_defs_file.symlink_to(rootdir / "lib" / "message_defs.capnp")
        except:
            if not message_defs_file.is_symlink():
                raise

        _cythonize = partial(
            cythonize,
            nthreads=int(os.environ.get("DRAGON_BUILD_NTHREADS", os.cpu_count() if platform.system() != "Darwin" else 0)),
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
            # build_py_options = self.distribution.command_options.setdefault('build_py', {})
            # build_py_options['compile'] = ('setup script', 1)
            build_ext_options = self.distribution.command_options.setdefault("build_ext", {})
            build_ext_options["inplace"] = ("setup script", 0)

        super().run()
        lib_tempdir.unlink()


class build_py(_build_py):
    # The find_modules() implementation in setuptools (really distutils)
    # automatically adds __init__.py for any non-root package module listed in
    # py_modules. As a result, our implementation is a simplified variant of
    # the original that does not automatically add __init__.py.

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


setup(
    # cmdclass={"build": build, "build_py": build_py, "clean": clean},
    cmdclass={"build": build, "build_py": build_py},
    options={"build_py": {"compile": 1}, "build_ext": {"inplace": 1}},
    name="dragonhpc",
    version=os.environ.get("DRAGON_VERSION", "latest"),
    packages=find_packages(),
    package_data={"dragon": ["lib/libdragon.so", "lib/libpmod.so"]},
    ext_modules=extensions,
    entry_points=entry_points,
    python_requires=">=3.10",
    install_requires=[
        "cloudpickle>=3.0.0",
        "gunicorn>=22.0.0",
        "flask>=3.0.3",
        "pyyaml>=6.0.2",
        "requests>=2.32.2",
        "psutil>=5.9.0",
        "pycapnp>=2.0.0,<2.2.0",
        "paramiko>=3.5.1",
        "flask-jwt-extended>=4.7.1",
        "networkx",
    ],
)
