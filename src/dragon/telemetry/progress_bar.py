import logging
import os
import subprocess
import sys
import time

from pathlib import Path
from signal import signal, SIGINT


log = logging.getLogger(__name__)


# global vars (needed for shutdown via sigterm)
keep_on_truckin = True
progress_bar_started = False


# this signal handler just sets a variable to indicate that
# the server should shut down
def were_done_here(signal, frame):
    global keep_on_truckin
    keep_on_truckin = False


# start progress bar server
def start(fe_server):
    global progress_bar_started

    if not progress_bar_started:
        try:
            sys_argv_str = ".".join(sys.argv)
            net = fe_server._populate_net_config()
            nnodes_user = fe_server.nnodes
            nnodes_wlm = net.allocation_nnodes

            app_config_str = f"{sys_argv_str}.{nnodes_user=}.{nnodes_wlm=}"

            os.environ["_DRAGON_HSTA_PROGRESS_BAR"] = app_config_str
            os.makedirs(".dragon", exist_ok=True)

            handle = subprocess.Popen(
                [
                    "python3",
                    "-c",
                    f'from dragon.telemetry.progress_bar import main_loop; main_loop("{app_config_str}")',
                ],
                stdout=sys.stdout,
            )

            progress_bar_started = True
            log.info("progress bar started")
            return handle
        except:
            # give user more immediate feedback on progress bar failure
            print("progress bar failed to start", file=sys.stderr, flush=True)
            log.info("progress bar failed to start")
            return None


# stop the server via sigterm
def stop(handle):
    if progress_bar_started is True:
        os.kill(handle.pid, SIGINT)
        handle.wait()
        log.info("progress bar stopped")


class ProgressBar:
    def __init__(self, app_config_str):
        self.progress_bar = None
        self.enabled = False
        self.update_interval = 1.0
        self.app_config_str = app_config_str
        self.old_val = 0
        self.enabled = False
        self.start_time = 0.0
        self.current_time = 0.0
        self.path_current = None
        self.progress_bar = None
        self.progress_update = None

    def __enter__(self):
        try:
            path_expected = Path(".dragon") / f"{self.app_config_str}.hsta-progress-expected.dat"
            self.path_current = Path(".dragon") / f"{self.app_config_str}.hsta-progress-current.dat"

            try:
                with open(path_expected, "r") as file_expected:
                    expected_str = file_expected.readline().strip()
                    if expected_str != "":
                        expected_val = int(expected_str)
                    else:
                        expected_val = -1
            except:
                expected_val = -1

            from alive_progress import alive_bar

            if expected_val > 0:
                self.progress_bar = alive_bar(expected_val)
            else:
                self.progress_bar = alive_bar()
            self.progress_update = self.progress_bar.__enter__()

            self.enabled = True
            self.start_time = time.time()
            self.current_time = time.time()
            log.info("successfully created progress bar")
        except:
            global keep_on_truckin
            keep_on_truckin = False
            # give user more immediate feedback on progress bar failure
            print("failed to create progress bar", file=sys.stderr, flush=True)
            log.info("failed to create progress bar")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # cleans up the progress bar (including removing it from the screen)
        if self.progress_bar is not None:
            self.progress_bar.__exit__(None, None, None)
            log.info("cleaned up progress bar")

    # read values from HSTA to update the progress bar
    def update(self):
        if self.enabled:
            self.current_time = time.time()
            if self.current_time - self.start_time > self.update_interval:
                try:
                    with open(self.path_current, "r") as file_current:
                        current_val = file_current.readline().strip()
                    if current_val != "":
                        current_val = int(current_val)
                        self.progress_update(current_val - self.old_val)
                        self.old_val = current_val
                        self.start_time = time.time()
                except:
                    pass


def main_loop(app_config_str):
    signal(SIGINT, were_done_here)

    with ProgressBar(app_config_str) as progress_bar:
        while keep_on_truckin:
            # Check for new completions from HSTA to update progress bar
            progress_bar.update()
            time.sleep(progress_bar.update_interval)
