#!/usr/bin/env python3

"""
Test script for launcher startup
"""

import dragon.globalservices.process_desc as proc
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.log_setup as log_setup
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as parms
import dragon.utils as du
import dragon.launcher.backend as backend
import dragon.launcher.frontend as frontend
import inspect
import logging
import multiprocessing as mp
import support.util as tsu
import unittest
import shim_dragon_paths

# required to get certain tests to run correctly
mp.set_start_method("spawn", force=True)


class StreamQueue:
    def __init__(self, *args, **kwargs):
        self.queue = mp.Queue(*args, **kwargs)

    def write(self, msg):
        self.queue.put(msg)

    def flush(self):
        pass

    def read(self):
        rv = ""

        while not self.queue.empty():
            rv += self.queue.get()

        return rv

    def readline(self):
        return self.read()

    def close(self):
        self.queue.close()
        self.queue.join_thread()


########################################################################
# This is the unit test file for the Launcher Dragon Run-time Services #
########################################################################
# The unit tests presented here are documented in comments in this file
# and further documentation can be found in the Dragon documentation
# under the "Dragon API and Infrastructure Testing" section. In addition,
# documentation on the Launcher design and functionality can be found
# under the section titled "The Launcher" in the "Internal Documentation"
# of the Dragon Runtime services.
########################################################################


def startup_and_run_frontend(launcher_queue, backend_queue, argv, stdout, stderr, name_addition=""):
    log_setup.setup_logging(basename="la_frontend_" + name_addition, the_level=logging.DEBUG)
    frontend.launcher_start(launcher_queue, backend_queue, argv=argv, stdout=stdout, stderr=stderr)


PDESC = proc.ProcessDescriptor
PRESP = dmsg.GSProcessCreateResponse
PRESPERR = dmsg.GSProcessCreateResponse.Errors


class FrontendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.launcher_main_rh, self.launcher_main_wh = mp.Pipe()
        self.backend_main_rh, self.backend_main_wh = mp.Pipe()
        self.stdout = StreamQueue()
        self.stderr = StreamQueue()

    def tearDown(self) -> None:
        self.launcher_main_rh.close()
        self.launcher_main_wh.close()
        self.backend_main_rh.close()
        self.backend_main_wh.close()
        self.stdout.close()
        self.stderr.close()

    def start_launcher(self, args=None):
        if args is None:
            args = []

        test_name = self.__class__.__name__ + "_" + inspect.stack()[2][0].f_code.co_name

        self.proc = mp.Process(
            target=startup_and_run_frontend,
            args=(self.launcher_main_rh, self.backend_main_wh, args, self.stdout, self.stderr),
            kwargs={"name_addition": test_name},
            daemon=False,
            name="launcher_fe",
        )

        self.proc.start()

    def do_bringup(self, argv=None):

        if argv is None:
            argv = []

        self.start_launcher(argv)
        self.launcher_main_wh.send(dmsg.GSIsUp(tag=0).serialize())

    def do_teardown(self, timeout=tsu.DEFAULT_TIMEOUT):
        # The teardown is initiated by an exit of the launcher. When this happens,
        # the launcher sends the GSTeardown message to initiate shutdown
        gs_teardown = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSTeardown, timeout=timeout)

        # Send the GSHalted
        self.launcher_main_wh.send(dmsg.GSHalted(tag=0).serialize())
        # Get the SHTeardown from the Launcher. All messages are routed
        # through the backend.
        la_broadcast = tsu.get_and_check_type(self.backend_main_rh, dmsg.LABroadcast, timeout=timeout)

        sh_teardown = dmsg.parse(la_broadcast.data)

        assert isinstance(sh_teardown, dmsg.SHTeardown), "Expected SHTeardown, got: " + repr(gs_teardown)

        # The rest of the single-node teardown sequence is carried out by the
        # backend code.

        logging.shutdown()

        # Wait for the launcher frontend to exit here so we know that
        # we have completely finished processing all messages before
        # trying to verify output in individual test cases below.
        self.proc.join(10)

        # If the frontend did not exit within 10 seconds, then something is wrong.
        self.assertTrue(self.proc.exitcode is not None)

    def test_bringup_teardown(self):
        # Tests normal bringup followed by teardown.

        self.do_bringup(argv=["-r", "launcher/file0.py"])

        self.do_teardown()

    def test_start_proc1(self):

        args = ["launcher/test.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0, ref=gs_process_join.tag, err=dmsg.GSProcessJoinResponse.Errors.SUCCESS
            ).serialize()
        )

        self.do_teardown(timeout=None)

    def test_start_proc2(self):

        args = ["-r", "launcher/file1.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        gs_process_kill = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessKill)

        self.launcher_main_wh.send(
            dmsg.GSProcessKillResponse(
                tag=0,
                ref=gs_process_kill.tag,
                err=dmsg.GSProcessKillResponse.Errors.UNKNOWN,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # Errors.SUCCESS
        # Errors.SUCCESS
        # Errors.SUCCESS
        # [Kill unsuccessful, error is: UNKNOWN]
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        # For future tests
        # ====================
        # use these two print statements to determine output, then assertEqual for the actual text as
        # it appears in this test.
        # print("stdout", stdout)
        # print("stderr", stderr)

        self.assertIn("SUCCESS", stdout)
        self.assertIn("True", stdout)
        self.assertIn("UNKNOWN", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_start_proc3(self):

        args = ["-r", "launcher/file1.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        gs_process_kill = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessKill)

        self.launcher_main_wh.send(
            dmsg.GSProcessKillResponse(
                tag=0,
                ref=gs_process_kill.tag,
                err=dmsg.GSProcessKillResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()
        stderr = self.stderr.read()

        self.assertIn("Errors.SUCCESS", stdout)
        self.assertIn("0", stdout)
        self.assertIn("True", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_start_proc4(self):
        args = ["-r", "launcher/file2.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        msg = dmsg.GSProcessJoinResponse(tag=0, ref=gs_process_join.tag, err=dmsg.GSProcessJoinResponse.Errors.SUCCESS)
        self.launcher_main_wh.send(msg.serialize())

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        stderr = self.stderr.read()

        self.assertIn("Hello World", stdout)
        self.assertIn("True", stdout)

        self.assertIn("Hello World", stderr)
        self.assertIn("stderr", stderr)

    def test_start_proc5(self):

        args = ["-r", "launcher/file3.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)
        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        msg = dmsg.SHFwdOutput(
            tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
        )
        self.launcher_main_wh.send(msg.serialize())

        msg = dmsg.SHFwdOutput(
            tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
        )
        self.launcher_main_wh.send(msg.serialize())

        msg = dmsg.GSProcessJoinResponse(tag=0, ref=gs_process_join.tag, err=dmsg.GSProcessJoinResponse.Errors.SUCCESS)
        self.launcher_main_wh.send(msg.serialize())

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # [stdout: p_uid=4] Hello World
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        # '''[stderr: p_uid=4] Hello World to stderr'''

        self.assertIn("SUCCESS", stdout)
        self.assertIn("True", stdout)
        self.assertIn("Hello World", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertIn("Hello World", stderr)
        self.assertIn("stderr", stderr)

    def test_start_proc6(self):
        args = ["-r", "launcher/file4.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)
        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Stop\n"
            ).serialize()
        )

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()
        # '''(4, 'SUCCESS', None)
        # Got pipe text: Hello World
        #
        # Got pipe text: Hello World to stderr
        #
        # Got pipe text: Stop
        #
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("4", stdout)
        self.assertIn("Hello World", stdout)
        self.assertIn("stderr", stdout)
        self.assertIn("Stop", stdout)
        self.assertIn("True", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_start_proc7(self):

        args = ["-r", "launcher/file1.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(dmsg.GSUnexpected(tag=0, ref=gs_process_join.tag).serialize())

        gs_process_kill = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessKill)

        self.launcher_main_wh.send(dmsg.GSUnexpected(tag=0, ref=gs_process_kill.tag).serialize())

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # Errors.SUCCESS
        # Errors.SUCCESS
        # Errors.SUCCESS
        # [Kill unsuccessful, error is: UNKNOWN]
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("SUCCESS", stdout)
        self.assertIn("True", stdout)
        self.assertIn("Kill unsuccessful", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_start_proc8(self):

        args = ["-r", "launcher/file8.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        self.launcher_main_wh.send(
            dmsg.GSProcessCreateResponse(
                tag=3, err=1, ref=gs_process_create.tag, err_info="[Errno 13] Permission denied: " "'python3'"
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''[Create unsuccessful, error is: FAIL]
        # p_uid is None
        # (None, 'FAIL', "[Errno 13] Permission denied: 'python3'")
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("FAIL", stdout)
        self.assertIn("None", stdout)
        self.assertIn("Permission denied", stdout)
        self.assertIn("True", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_start_server(self):

        args = ["-r", "launcher/file5.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.launcher_main_wh.send(dmsg.LAPassThruBF(tag=0, data="PassThru Message").serialize())

        lapassthrufb = tsu.get_and_check_type(self.backend_main_rh, dmsg.LAPassThruFB)

        self.assertEqual(lapassthrufb.data, "PassThru Message")

        self.launcher_main_wh.send(dmsg.LAPassThruBF(tag=0, data="Another Message").serialize())

        lapassthrufb = tsu.get_and_check_type(self.backend_main_rh, dmsg.LAPassThruFB)

        self.assertEqual(lapassthrufb.data, "Another Message")

        self.launcher_main_wh.send(dmsg.LAPassThruBF(tag=0, data="Exit").serialize())

        self.launcher_main_wh.send(
            dmsg.LAServerModeExit(
                tag=0, ref=gs_process_create.tag, err=dmsg.LAServerModeExit.Errors.SUCCESS
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()
        # '''[stdout: p_uid=4] Hello World
        # Server Mode Exited
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("Hello", stdout)
        self.assertIn("Server", stdout)
        self.assertIn("Exited", stdout)
        self.assertIn("True", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_proc_list(self):

        args = ["-r", "launcher/file6.py"]

        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Stop\n"
            ).serialize()
        )

        gs_process_join = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        gs_process_list = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessList)

        self.launcher_main_wh.send(
            dmsg.GSProcessListResponse(
                tag=0, ref=gs_process_list.tag, err=dmsg.GSProcessListResponse.Errors.SUCCESS, plist=[("4", "4-name")]
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': None}}
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': None}}
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': 0}}
        # {'4': '4-name'}
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("python3", stdout)
        self.assertIn("exit_code", stdout)
        self.assertIn("4-name", stdout)
        self.assertIn("True", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_send(self):

        args = ["-r", "launcher/file7.py"]

        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.backend_main_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.launcher_main_wh.send(msg.serialize())

        sh_process_input = tsu.get_and_check_type(self.backend_main_rh, dmsg.SHFwdInput)

        self.launcher_main_wh.send(
            dmsg.SHFwdInputErr(tag=0, ref=sh_process_input.tag, err=dmsg.SHFwdInputErr.Errors.SUCCESS).serialize()
        )

        self.assertEqual(sh_process_input.input, "Hello World")

        sh_process_input2 = tsu.get_and_check_type(self.backend_main_rh, dmsg.SHFwdInput)

        self.launcher_main_wh.send(
            dmsg.SHFwdInputErr(tag=0, ref=sh_process_input2.tag, err=dmsg.SHFwdInputErr.Errors.SUCCESS).serialize()
        )

        self.assertEqual(sh_process_input2.input, "Goodbye")

        self.launcher_main_wh.send(dmsg.LADumpState(tag=0).serialize())

        self.launcher_main_wh.send(dmsg.LADumpState(tag=0, filename="ladumptest.log").serialize())

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': None}}
        # response from send (<Errors.SUCCESS: 0>, '')
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("4", stdout)
        self.assertIn("python3", stdout)
        self.assertIn("True", stdout)
        self.assertIn("Exit", stdout)

        self.assertEqual(stderr.strip(), "")


def startup_and_run_backend(
    incoming_channel_queue=None,
    incoming_frontend_queue=None,
    incoming_l0_queue=None,
    frontend_queue=None,
    gs_queue=None,
    sh_queue=None,
    sh_l0_queue=None,
    all_other_channels=None,
    name_addition="",
):
    log_setup.setup_logging(basename="la_backend_" + name_addition, the_level=logging.DEBUG)

    backend.backend_launcher_start(
        incoming_channel_queue,
        incoming_frontend_queue,
        incoming_l0_queue,
        frontend_queue,
        gs_queue,
        sh_queue,
        sh_l0_queue,
        all_other_channels,
        mode=dfacts.TEST_MODE,
    )


class BackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.launcher_main_rh, self.launcher_main_wh = mp.Pipe()
        self.backend_main_rh, self.backend_main_wh = mp.Pipe()
        self.main_queue_rh, self.main_queue_wh = mp.Pipe()
        self.from_frontend_queue_rh, self.from_frontend_queue_wh = mp.Pipe()
        self.l0_queue_rh, self.l0_queue_wh = mp.Pipe()
        self.to_frontend_queue_rh, self.to_frontend_queue_wh = mp.Pipe()
        self.gs_queue_rh, self.gs_queue_wh = mp.Pipe()
        self.sh_queue_rh, self.sh_queue_wh = mp.Pipe()
        self.sh_l0_queue_rh, self.sh_l0_queue_wh = mp.Pipe()
        self.all_other_channels_rh, self.all_other_channels_wh = mp.Pipe()
        self.proc = None

    def tearDown(self) -> None:
        self.launcher_main_rh.close()
        self.launcher_main_wh.close()
        self.backend_main_rh.close()
        self.backend_main_wh.close()
        self.main_queue_rh.close()
        self.main_queue_wh.close()
        self.from_frontend_queue_rh.close()
        self.from_frontend_queue_wh.close()
        self.l0_queue_rh.close()
        self.l0_queue_wh.close()
        self.to_frontend_queue_rh.close()
        self.to_frontend_queue_wh.close()
        self.gs_queue_rh.close()
        self.gs_queue_wh.close()
        self.sh_queue_rh.close()
        self.sh_queue_wh.close()
        self.sh_l0_queue_rh.close()
        self.sh_l0_queue_wh.close()
        self.all_other_channels_rh.close()
        self.all_other_channels_wh.close()
        if self.proc is not None:
            self.proc.join(10)
            # If the backend did not exit within 10 seconds, then something is wrong.
            self.assertTrue(self.proc.exitcode is not None)

    def start_launcher_backend(self):
        if self.proc is not None:
            self.proc.join(10)
            # If the backend did not exit within 10 seconds, then something is wrong.
            self.assertTrue(self.proc.exitcode is not None)

        test_name = self.__class__.__name__ + "_" + inspect.stack()[2][0].f_code.co_name

        self.proc = mp.Process(
            target=startup_and_run_backend,
            args=(
                self.launcher_main_rh,
                self.from_frontend_queue_rh,
                self.l0_queue_rh,
                self.to_frontend_queue_wh,
                self.gs_queue_wh,
                self.sh_queue_wh,
                self.sh_l0_queue_wh,
                self.all_other_channels_wh,
            ),
            kwargs={"name_addition": test_name},
            daemon=False,
            name="backend",
        )

        self.proc.start()

    def do_bringup(self):
        # Start up the launcher backend by itself.
        self.start_launcher_backend()

        # do the message bringup sequence.
        # Get the node index message from the backend.
        tsu.get_and_check_type(self.sh_l0_queue_rh, dmsg.BENodeIdxSH)

        # Send the BEPingSH as part of the single node startup sequence
        self.l0_queue_wh.send(
            dmsg.SHPingBE(tag=0, shep_cd="", be_cd="", gs_cd="", default_pd="", inf_pd="").serialize()
        )

        # Then the Shepherd is sent the SHPingBE message
        tsu.get_and_check_type(self.sh_queue_rh, dmsg.BEPingSH)

        # Next, SHChannelsUp
        self.launcher_main_wh.send(
            dmsg.SHChannelsUp(
                tag=0,
                host_id="ubuntu",
                ip_addrs=["127.0.0.1"],
                shep_cd=du.B64.bytes_to_str(b""),
                gs_cd=du.B64.bytes_to_str(b""),
            ).serialize()
        )

        # Next, GSIsUp
        self.launcher_main_wh.send(dmsg.GSIsUp(tag=0).serialize())

        # And, it comes out to the launcher front end.
        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.GSIsUp)

    def do_teardown(self):
        # The teardown is initiated by an exit of the launcher. When this happens,
        # the launcher sends the GSTeardown message to initiate shutdown
        self.launcher_main_wh.send(dmsg.GSTeardown(tag=0).serialize())

        # Check that it gets routed to Global Services
        tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSTeardown)

        # Send the GSHalted
        self.launcher_main_wh.send(dmsg.GSHalted(tag=0).serialize())

        # Check that it gets routed to Front End
        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.GSHalted)

        # Get the SHTeardown from the Launcher. All messages are routed
        # through the backend.
        self.launcher_main_wh.send(dmsg.LABroadcast(tag=0, data=dmsg.SHTeardown(tag=0).serialize()).serialize())

        # See that the SHTeardown gets routed to the Shepherd.
        tsu.get_and_check_type(self.sh_queue_rh, dmsg.SHTeardown)

        # Send the SHHaltBE to the backend only. The backend responds to this one.
        self.launcher_main_wh.send(dmsg.SHHaltBE(tag=0).serialize())

        # Check to see the BEHalted got written to the Shepherd stdin.
        tsu.get_and_check_type(self.sh_l0_queue_rh, dmsg.BEHalted)

        # Send the SHHalted Message
        self.l0_queue_wh.send(dmsg.SHHalted(tag=0).serialize())

        # At this point the Backend should exit.
        logging.shutdown()

    def test_bringup_teardown(self):
        self.do_bringup()
        self.do_teardown()

    def test_message_routes(self):
        self.do_bringup()

        self.launcher_main_wh.send(
            dmsg.GSProcessCreate(
                tag=1, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID, exe="launcher/test.py", args=[]
            ).serialize()
        )

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)
        proc_desc = proc.ProcessDescriptor(p_uid=4, name="TestProc", node=1, p_p_uid=0)
        msg = dmsg.GSProcessCreateResponse(
            tag=0, ref=gs_process_create.tag, err=dmsg.GSProcessCreateResponse.Errors.SUCCESS, desc=proc_desc
        )
        self.launcher_main_wh.send(msg.serialize())

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.GSProcessCreateResponse)

        self.launcher_main_wh.send(
            dmsg.GSProcessKill(
                tag=1, t_p_uid=4, p_uid=dfacts.LAUNCHER_PUID, r_c_uid=dfacts.BASE_BE_CUID, sig=9
            ).serialize()
        )

        tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessKill)

        self.launcher_main_wh.send(
            dmsg.GSProcessKillResponse(
                tag=0, ref=4, err=dmsg.GSProcessKillResponse.Errors.UNKNOWN, exit_code=0
            ).serialize()
        )

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.GSProcessKillResponse)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoin(tag=1, p_uid=dfacts.LAUNCHER_PUID, t_p_uid=4, r_c_uid=dfacts.BASE_BE_CUID).serialize()
        )

        tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.launcher_main_wh.send(
            dmsg.GSProcessJoinResponse(tag=0, ref=1, err=dmsg.GSProcessJoinResponse.Errors.SUCCESS).serialize()
        )

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.GSProcessJoinResponse)

        self.launcher_main_wh.send(
            dmsg.GSProcessList(tag=0, r_c_uid=dfacts.BASE_BE_CUID, p_uid=dfacts.LAUNCHER_PUID).serialize()
        )

        gs_process_list = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessList)

        self.launcher_main_wh.send(
            dmsg.GSProcessListResponse(
                tag=0, ref=gs_process_list.tag, err=dmsg.GSProcessListResponse.Errors.SUCCESS, plist=[("4", "4-name")]
            ).serialize()
        )

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.GSProcessListResponse)

        self.launcher_main_wh.send(
            dmsg.SHFwdInput(
                tag=0, p_uid=dfacts.LAUNCHER_PUID, t_p_uid=4, r_c_uid=dfacts.BASE_BE_CUID, input="Hello World"
            ).serialize()
        )

        tsu.get_and_check_type(self.sh_queue_rh, dmsg.SHFwdInput)

        self.launcher_main_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.SHFwdOutput)

        self.launcher_main_wh.send(dmsg.LAPassThruBF(tag=0, data="PassThru Message").serialize())

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.LAPassThruBF)

        self.launcher_main_wh.send(dmsg.LAPassThruFB(tag=0, c_uid=5, data="hello world").serialize())

        tsu.get_and_check_type(self.all_other_channels_rh, dmsg.LAPassThruFB)

        self.launcher_main_wh.send(
            dmsg.LAServerModeExit(tag=0, ref=0, err=dmsg.LAServerModeExit.Errors.SUCCESS).serialize()
        )

        tsu.get_and_check_type(self.to_frontend_queue_rh, dmsg.LAServerModeExit)

        self.do_teardown()


class CombinedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backend_channel_rh, self.backend_channel_wh = mp.Pipe()
        self.backend_to_frontend_rh, self.backend_to_frontend_wh = mp.Pipe()
        self.frontend_to_backend_rh, self.frontend_to_backend_wh = mp.Pipe()
        self.l0_queue_rh, self.l0_queue_wh = mp.Pipe()
        self.gs_queue_rh, self.gs_queue_wh = mp.Pipe()
        self.sh_queue_rh, self.sh_queue_wh = mp.Pipe()
        self.sh_l0_queue_rh, self.sh_l0_queue_wh = mp.Pipe()
        self.all_other_channels_rh, self.all_other_channels_wh = mp.Pipe()
        self.stdout = StreamQueue()
        self.stderr = StreamQueue()

    def tearDown(self) -> None:
        self.backend_channel_rh.close()
        self.backend_channel_wh.close()
        self.backend_to_frontend_rh.close()
        self.backend_to_frontend_wh.close()
        self.frontend_to_backend_rh.close()
        self.frontend_to_backend_wh.close()
        self.l0_queue_rh.close()
        self.l0_queue_wh.close()
        self.gs_queue_rh.close()
        self.gs_queue_wh.close()
        self.sh_queue_rh.close()
        self.sh_queue_wh.close()
        self.sh_l0_queue_rh.close()
        self.sh_l0_queue_wh.close()
        self.all_other_channels_rh.close()
        self.all_other_channels_wh.close()

    def start_combined(self, args=None):
        if args is None:
            args = []
        test_name = self.__class__.__name__ + "_" + inspect.stack()[2][0].f_code.co_name

        self.backend = mp.Process(
            target=startup_and_run_backend,
            args=(
                self.backend_channel_rh,
                self.frontend_to_backend_rh,
                self.l0_queue_rh,
                self.backend_to_frontend_wh,
                self.gs_queue_wh,
                self.sh_queue_wh,
                self.sh_l0_queue_wh,
                self.all_other_channels_wh,
            ),
            kwargs={"name_addition": test_name},
            daemon=False,
            name="backend",
        )

        self.backend.start()

        self.frontend = mp.Process(
            target=startup_and_run_frontend,
            args=(self.backend_to_frontend_rh, self.frontend_to_backend_wh, args, self.stdout, self.stderr),
            kwargs={"name_addition": test_name},
            daemon=False,
            name="frontend",
        )

        self.frontend.start()

    def do_bringup(self, argv=None):
        # This is executed to do the bring up of the Shepherd in test. Before this
        # executes in any test, the setUp method has already run. This creates
        # the Shepherd as a multiprocessing Process with all the connections into
        # it for unit testing.
        if argv is None:
            argv = []

        self.start_combined(argv)

        # To finish bringup the sequence of bringup messages are initiated and
        # checked with the Shepherd.
        # Get the node index message from the backend.
        tsu.get_and_check_type(self.sh_l0_queue_rh, dmsg.BENodeIdxSH)

        # Send the BEPingSH as part of the single node startup sequence
        self.l0_queue_wh.send(
            dmsg.SHPingBE(tag=0, shep_cd="", be_cd="", gs_cd="", default_pd="", inf_pd="").serialize()
        )

        # First receive the SHPingBE message
        tsu.get_and_check_type(self.sh_queue_rh, dmsg.BEPingSH)

        # Send the SHChannelsUp to complete the Shepherd part of the sequence
        up_msg = dmsg.SHChannelsUp(
            tag=0,
            host_id="ubuntu",
            ip_addrs=["127.0.0.1"],
            shep_cd=du.B64.bytes_to_str(b""),
            gs_cd=du.B64.bytes_to_str(b""),
        )
        self.backend_channel_wh.send(up_msg.serialize())

        # Send the GSIsUp to complete the sequence
        self.backend_channel_wh.send(dmsg.GSIsUp(tag=0).serialize())

    def do_teardown(self, timeout=tsu.DEFAULT_TIMEOUT):
        # The teardown is initiated by an exit of the launcher. When this happens,
        # the launcher sends the GSTeardown message to initiate shutdown
        tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSTeardown, timeout=timeout)

        # Send the GSHalted
        self.backend_channel_wh.send(dmsg.GSHalted(tag=0).serialize())

        # Get the SHTeardown from the Launcher. All messages are routed
        # through the backend.
        tsu.get_and_check_type(self.sh_queue_rh, dmsg.SHTeardown, timeout=timeout)

        # The rest of the single-node teardown sequence is carried out by the
        # backend code.

        # Send the SHHaltBE to the backend only. The backend responds to this one.
        self.backend_channel_wh.send(dmsg.SHHaltBE(tag=0).serialize())

        # Check to see the BEHalted got written to the Shepherd stdin.
        tsu.get_and_check_type(self.sh_l0_queue_rh, dmsg.BEHalted)

        # Send the SHHalted Message
        self.l0_queue_wh.send(dmsg.SHHalted(tag=0).serialize())

        logging.shutdown()

        # wait for the frontend to exit before returning
        # from the do_teardown so output can be verified by the
        # test case.
        self.frontend.join(10)
        # If the frontend did not exit within 10 seconds, then something is wrong.
        self.assertTrue(self.frontend.exitcode is not None)

    def test_bringup_teardown(self):
        # Tests normal bringup followed by teardown.

        self.do_bringup(argv=["-r", "launcher/file0.py"])

        self.do_teardown()

        stdout = self.stdout.read()
        # '''True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        # removes all whitespace, but order is enforced within the output
        self.assertIn("True", stdout)
        self.assertIn("Exiting", stdout)
        self.assertEqual("", stderr.strip())

    def test_start_proc1(self):
        args = ["launcher/test.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()
        # '''[Exiting Launcher]'''

        stderr = self.stderr.read()
        self.assertIn("Exiting", stdout)

        self.assertEqual("", stderr.strip())

    def test_start_proc2(self):
        args = ["-r", "launcher/file1.py"]

        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        gs_process_kill = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessKill)

        self.backend_channel_wh.send(
            dmsg.GSProcessKillResponse(
                tag=0,
                ref=gs_process_kill.tag,
                err=dmsg.GSProcessKillResponse.Errors.UNKNOWN,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # Errors.SUCCESS
        # Errors.SUCCESS
        # Errors.SUCCESS
        # [Kill unsuccessful, error is: UNKNOWN]
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("SUCCESS", stdout)
        self.assertIn("Kill", stdout)
        self.assertIn("unsuccessful", stdout)
        self.assertIn("True", stdout)

        self.assertEqual("", stderr.strip())

    def test_start_proc3(self):
        args = ["-r", "launcher/file1.py"]

        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        gs_process_kill = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessKill)

        self.backend_channel_wh.send(
            dmsg.GSProcessKillResponse(
                tag=0,
                ref=gs_process_kill.tag,
                err=dmsg.GSProcessKillResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # Errors.SUCCESS
        # Errors.SUCCESS
        # Errors.SUCCESS
        # Killed it
        # Exit Code is 0
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("SUCCESS", stdout)
        self.assertIn("Killed", stdout)
        self.assertIn("0", stdout)
        self.assertIn("True", stdout)
        self.assertEqual("", stderr.strip())

    def test_start_proc4(self):
        args = ["-r", "launcher/file2.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        self.assertIn("Hello World", stdout)
        self.assertIn("True", stdout)

    def test_start_proc5(self):
        args = ["-r", "launcher/file3.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # [stdout: p_uid=4] Hello World
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        # '[stderr: p_uid=4] Hello World to stderr'

        self.assertIn("SUCCESS", stdout)
        self.assertIn("True", stdout)

        self.assertIn("Hello", stderr)

    def test_start_proc6(self):
        args = ["-r", "launcher/file4.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Stop\n"
            ).serialize()
        )

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # expecting:
        # (4, 'SUCCESS', None)
        # Got pipe text: Hello World
        #
        # Got pipe text: Hello World to stderr
        #
        # Got pipe text: Stop
        #
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("4", stdout)
        self.assertIn("Hello World", stdout)
        self.assertIn("stderr", stdout)
        self.assertIn("Stop", stdout)
        self.assertIn("True", stdout)
        self.assertIn("[Exiting Launcher]", stdout)

        self.assertEqual(stderr.strip(), "")

    def test_start_proc7(self):
        args = ["-r", "launcher/file1.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(dmsg.GSUnexpected(tag=0, ref=gs_process_join.tag).serialize())

        gs_process_kill = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessKill)

        self.backend_channel_wh.send(dmsg.GSUnexpected(tag=0, ref=gs_process_kill.tag).serialize())

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # Errors.SUCCESS
        # Errors.SUCCESS
        # Errors.SUCCESS
        # [Kill unsuccessful, error is: UNKNOWN]
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("SUCCESS", stdout)
        self.assertIn("Kill", stdout)
        self.assertIn("unsuccessful", stdout)
        self.assertIn("UNKNOWN", stdout)
        self.assertIn("True", stdout)

        self.assertEqual("", stderr.strip())

    def test_start_server(self):
        args = ["-r", "launcher/file5.py"]
        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.backend_channel_wh.send(dmsg.LAPassThruBF(tag=0, data="PassThru Message").serialize())

        lapassthrufb = tsu.get_and_check_type(self.all_other_channels_rh, dmsg.LAPassThruFB)

        self.assertEqual(lapassthrufb.data, "PassThru Message")

        self.backend_channel_wh.send(dmsg.LAPassThruBF(tag=0, data="Another Message").serialize())

        lapassthrufb = tsu.get_and_check_type(self.all_other_channels_rh, dmsg.LAPassThruFB)

        self.assertEqual(lapassthrufb.data, "Another Message")

        self.backend_channel_wh.send(dmsg.LAPassThruBF(tag=0, data="Exit").serialize())

        self.backend_channel_wh.send(
            dmsg.LAServerModeExit(
                tag=0, ref=gs_process_create.tag, err=dmsg.LAServerModeExit.Errors.SUCCESS
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()
        # '''[stdout: p_uid=4] Hello World
        # Server Mode Exited
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("Hello", stdout)
        self.assertIn("World", stdout)
        self.assertIn("Server", stdout)
        self.assertIn("Mode", stdout)
        self.assertIn("True", stdout)

        self.assertEqual("", stderr.strip())

    def test_proc_list(self):
        args = ["-r", "launcher/file6.py"]

        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_uid=4, name="TestProc", node=1, p_p_uid=0)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Hello World\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDERR.value, data="Hello World to stderr\n"
            ).serialize()
        )

        self.backend_channel_wh.send(
            dmsg.SHFwdOutput(
                tag=0, p_uid="4", idx="0", fd_num=dmsg.SHFwdOutput.FDNum.STDOUT.value, data="Stop\n"
            ).serialize()
        )

        gs_process_join = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessJoin)

        self.backend_channel_wh.send(
            dmsg.GSProcessJoinResponse(
                tag=0,
                ref=gs_process_join.tag,
                err=dmsg.GSProcessJoinResponse.Errors.SUCCESS,
            ).serialize()
        )

        gs_process_list = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessList)

        self.backend_channel_wh.send(
            dmsg.GSProcessListResponse(
                tag=0, ref=gs_process_list.tag, err=dmsg.GSProcessListResponse.Errors.SUCCESS, plist=[("4", "4-name")]
            ).serialize()
        )

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()
        # '''(4, 'SUCCESS', None)
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': None}}
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': None}}
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': 0}}
        # {'4': '4-name'}
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("python3", stdout)
        self.assertIn("test.py", stdout)
        self.assertIn("env", stdout)
        self.assertIn("0", stdout)
        self.assertIn("True", stdout)

        self.assertEqual("", stderr.strip())

    def test_send(self):
        args = ["-r", "launcher/file7.py"]

        self.do_bringup(argv=args)

        gs_process_create = tsu.get_and_check_type(self.gs_queue_rh, dmsg.GSProcessCreate)

        proc_desc = PDESC(p_p_uid=0, p_uid=4, name="TestProc", node=1)
        msg = PRESP(tag=0, ref=gs_process_create.tag, err=PRESPERR.SUCCESS, desc=proc_desc)
        self.backend_channel_wh.send(msg.serialize())

        sh_fwd_input = tsu.get_and_check_type(self.sh_queue_rh, dmsg.SHFwdInput)

        self.backend_channel_wh.send(
            dmsg.SHFwdInputErr(tag=0, ref=sh_fwd_input.tag, err=dmsg.SHFwdInputErr.Errors.SUCCESS).serialize()
        )

        self.assertEqual(sh_fwd_input.input, "Hello World")

        sh_fwd_input2 = tsu.get_and_check_type(self.sh_queue_rh, dmsg.SHFwdInput)

        self.backend_channel_wh.send(
            dmsg.SHFwdInputErr(
                tag=0, ref=sh_fwd_input2.tag, err=dmsg.SHFwdInputErr.Errors.FAIL, err_info="Error Information"
            ).serialize()
        )

        self.assertEqual(sh_fwd_input2.input, "Goodbye")

        self.backend_channel_wh.send(dmsg.LADumpState(tag=0).serialize())

        self.backend_channel_wh.send(dmsg.LADumpState(tag=0, filename="ladumptest.log").serialize())

        self.do_teardown(timeout=None)

        stdout = self.stdout.read()

        # '''(4, 'SUCCESS', None)
        # {'4': {'exe': 'python3', 'args': ['test.py'], 'env': {}, 'run_dir': '', 'user_name': '', 'create_rc': 0,
        # 'exit_code': None}}
        # response from send (<Errors.FAIL: 1>, 'Error Information')
        # True
        # [Exiting Launcher]'''

        stderr = self.stderr.read()

        self.assertIn("4", stdout)
        self.assertIn("SUCCESS", stdout)
        self.assertIn("True", stdout)
        self.assertEqual("", stderr.strip())


if __name__ == "__main__":
    unittest.main()
