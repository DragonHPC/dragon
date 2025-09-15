import sys
import os
import dragon.globalservices.process as gproc
import dragon.launcher.launch_selector as launcher
import pathlib
import subprocess

def check_for_jupyter():
    try:
        command = ["ipython", "locate", "profile"]
        result = subprocess.run(command, capture_output=True, text=True)
        return result
    except FileNotFoundError:
        print("You must install jupyter to run dragon-jupyter. Install it with this command.", flush=True)
        print("python3 -m pip install jupyter")
        sys.exit(-1)

def main():
    try:
        result = check_for_jupyter()
        if result.returncode == 0:
            iPython_dir = str(result.stdout).strip()
        else:
            print(f"Could not find iPython profile directory:")
            print(result.stderr)
            sys.exit(-1)

        filename = pathlib.Path(iPython_dir)/"startup"/"00-dragon-jupyter-startup.py"

        # this is for kubernetes and only when we provide a pre-determined token
        token = os.getenv("K8S_JUPYTER_TOKEN")
        if token is not None:
            args = [
                "jupyter",
                "notebook",
                "--NotebookApp.token=" + token,
                "--ip",
                "0.0.0.0",
                "--no-browser",
                "--allow-root",
            ]
        else:
            args = ["jupyter", "notebook", "--ip", "0.0.0.0", "--no-browser", "--allow-root"]

        with open(filename,"w") as f:
            f.write(f"import os\n")
            f.write(f"if os.getenv('DRAGON_GS_RET_CD') is not None:\n")
            f.write(f"    os.environ['{gproc.DRAGON_CAPTURE_MP_CHILD_OUTPUT}'] = 'True'\n")
            f.write(f"    import dragon.globalservices.api_setup as api\n")
            f.write(f"    api.connect_to_infrastructure(force=True)\n")

        proc = subprocess.Popen(args)
        proc.wait()
    except FileNotFoundError:
        print("You must install jupyter to run dragon-jupyter. Install it with this command.", flush=True)
        print("python3 -m pip install jupyter")
        sys.exit(-1)

def start_server():
    check_for_jupyter()
    sys.argv = ["dragon"] + sys.argv[1:] + [__file__]
    sys.exit(launcher.main())

if __name__ == "__main__":
    main()
