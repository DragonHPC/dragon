import dragon
import sys
import dragon.globalservices.process as gproc
from jupyter_core.command import main

def startup():
    gproc.start_capturing_child_mp_output()
    sys.argv = ["jupyter", "notebook",  "--ip", "0.0.0.0", "--no-browser", "--allow-root"]
    rc = main()
    sys.exit(rc)

if __name__ == '__main__':
    startup()
