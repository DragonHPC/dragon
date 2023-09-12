import pathlib
import sys
import os


# Needed by tests spawning a new python process by executable name
orig_path = os.getenv('PATH')
py_exe_path = str(pathlib.Path(sys.executable).parent)
os.putenv('PATH', py_exe_path + ':' + orig_path)


# Needed if no PYTHONPATH is set to point at dragon/src or dragon not installed in site-packages
dragon_path = pathlib.Path(__file__).parent / '..' / 'src'
support_path = pathlib.Path(__file__).parent
if not dragon_path.exists():
    # Sometimes needed by fixtures such as launcher/fe_server.py
    dragon_path = pathlib.Path(__file__).parent / '..' / '..' / 'src'
    support_path = pathlib.Path(__file__).parent / '..'

for addl_path in (support_path, dragon_path):
    addl_abspath = str(addl_path.absolute())
    if addl_abspath not in sys.path:
        sys.path.append(addl_abspath)
