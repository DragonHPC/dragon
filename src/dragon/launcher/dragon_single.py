#!/usr/bin/env python3
"""Simple single node dragon infrastructure startup"""
import time

import os
import sys

import logging
import shutil
import threading

import dragon.channels as dch

import dragon.localservices.local_svc as dsls
import dragon.globalservices.server as dgs
import dragon.infrastructure.debug_support as dds
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.parameters as dparm
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.util as dutil
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.process_desc as pdesc
import dragon.launcher.launchargs as launchargs
import dragon.dlogging.util as dlog
import dragon.launcher.util as dlutil
import dragon.utils as du

# TODO: consider ways to bypass serialization when it isn't really needed

LAUNCHER_FAIL_EXIT = 1

# general amount of patience we have for an expected message
# in startup or teardown before we assume something has gone wrong
TIMEOUT_PATIENCE = 1  # seconds, 1 second.

# time to yield to let other things happen esp in unexpected shutdowns.
TIMEOUT_YIELD = 0.200  # seconds, 200 milliseconds

LOGBASE = "launcher"


def ls_start(ls_args):
    log = logging.getLogger(LOGBASE).getChild("ls_start")
    try:
        dsls.single(**ls_args)
        log.info("normal exit")
    except RuntimeError:
        log.exception("fail exit")


def build_stdmsg(msg, arg_map, is_stdout=True):
    if arg_map["no_label"]:
        return f"{msg.data}"

    msg_str = ""
    if is_stdout:
        msg_str += "[stdout: "
    else:
        msg_str += "[stderr: "

    if arg_map["verbose_label"]:
        msg_str += f"PID {msg.pid} @ {msg.hostname}]"
    elif arg_map["basic_label"]:
        msg_str += f"Dragon PID {msg.p_uid}]"

    msg_str += f" {msg.data}"
    return msg_str


def output_monitor(la_in):
    arg_map = launchargs.get_args()  # Get args once to check decoration options
    while True:
        msg = dmsg.parse(la_in.recv())
        if isinstance(msg, dmsg.SHFwdOutput):
            if msg.fd_num == msg.FDNum.STDOUT.value:
                msg_str = build_stdmsg(msg, arg_map, True)
                sys.stdout.write(msg_str)
                sys.stdout.flush()
            elif msg.fd_num == msg.FDNum.STDERR.value:
                msg_str = build_stdmsg(msg, arg_map, False)
                sys.stderr.write(msg_str)
                sys.stderr.flush()
        elif isinstance(msg, dmsg.GSProcessCreateResponse):
            if msg.err != msg.Errors.SUCCESS:
                print("+++ head proc did not start")
                return LAUNCHER_FAIL_EXIT
        elif isinstance(msg, dmsg.GSHeadExit):
            print("+++ head proc exited, code {}".format(msg.exit_code))
            return msg.exit_code
        elif isinstance(msg, dmsg.Breakpoint):
            log = logging.getLogger(LOGBASE).getChild("breakpoint")
            log.info(f"p_uid {msg.p_uid} node {msg.index}")
            dds.handle_breakpoint(msg)
        elif isinstance(msg, dmsg.AbnormalTermination):
            print(f"\n+++ Abnormal Termination of Dragon run-time with message:\n{msg.err_info}", flush=True)
            return -1 # Something else?
        else:
            print("unexpected message: {}".format(msg))


def shutdown_monitor(la_in):
    while la_in.poll(timeout=TIMEOUT_PATIENCE):
        msg = dmsg.parse(la_in.recv())
        if isinstance(msg, dmsg.SHFwdOutput):
            if msg.fd_num == msg.FDNum.STDOUT.value:
                sys.stdout.write(msg.data)
                sys.stdout.flush()
            elif msg.fd_num == msg.FDNum.STDERR.value:
                sys.stderr.write(msg.data)
                sys.stderr.flush()
        else:
            return msg

    raise TimeoutError()


def main():
    arg_map = launchargs.get_args()

    try:
        runtime_ip_addr = dutil.get_external_ip_addr().split(":")[0]
    except OSError:
        runtime_ip_addr = None

    if runtime_ip_addr is not None:
        os.environ["DRAGON_FE_EXTERNAL_IP_ADDR"] = runtime_ip_addr
        os.environ["DRAGON_HEAD_NODE_IP_ADDR"] = runtime_ip_addr
        os.environ["DRAGON_RT_UID"] = str(dutil.rt_uid_from_ip_addrs(runtime_ip_addr, runtime_ip_addr))

    dlog.setup_FE_logging(log_device_level_map=arg_map["log_device_level_map"], basename="dragon", basedir=os.getcwd())

    log = logging.getLogger(LOGBASE).getChild("main")
    log.info(f"start in pid {os.getpid()}, pgid {os.getpgid(0)}")

    start_msg = dlutil.mk_head_proc_start_msg()

    ls_stdin = dlutil.SRQueue()
    ls_stdout = dlutil.SRQueue()
    ls_args = {"ls_stdin": ls_stdin, "ls_stdout": ls_stdout}

    ls_thread = threading.Thread(name="local services", target=ls_start, args=(ls_args,), daemon=True)

    shm_status = dutil.survey_dev_shm()

    try:  # ls startup
        ls_thread.start()
        ls_stdin.send(dmsg.BENodeIdxSH(tag=dlutil.next_tag(), node_idx=0).serialize())
        be_ping = dmsg.parse(ls_stdout.recv())
        assert isinstance(be_ping, dmsg.SHPingBE)

        ls_in_ch = dch.Channel.attach(du.B64.str_to_bytes(be_ping.shep_cd))
        la_in_ch = dch.Channel.attach(du.B64.str_to_bytes(be_ping.be_cd))
        gs_in_ch = dch.Channel.attach(du.B64.str_to_bytes(be_ping.gs_cd))

        ls_in_wh = dconn.Connection(outbound_initializer=ls_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)
        la_in_rh = dconn.Connection(inbound_initializer=la_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)
        gs_in_wh = dconn.Connection(outbound_initializer=gs_in_ch, policy=dparm.POLICY_INFRASTRUCTURE)

        ls_in_wh.send(dmsg.BEPingSH(tag=dlutil.next_tag()).serialize())

        # Set a long timeout for DST
        ch_up = dlutil.get_with_timeout(la_in_rh, timeout=120)

        assert isinstance(ch_up, dmsg.SHChannelsUp)
    except (AssertionError, dch.ChannelError, TimeoutError) as err:
        log.exception("ls startup")
        print(f"ls startup failed:\n{err}")
        dutil.compare_dev_shm(shm_status)
        return LAUNCHER_FAIL_EXIT

    try:  # gs startup
        gs_thread = threading.Thread(name="global services", target=dgs.single_thread, args=(ls_in_wh,), daemon=True)
        gs_thread.start()

        # Set a long timeout for DST
        gs_up = dlutil.get_with_timeout(la_in_rh, timeout=120)
        assert isinstance(gs_up, dmsg.GSIsUp)
        gs_in_wh.send(start_msg.serialize())
    except (AssertionError, TimeoutError) as err:
        log.exception("gs startup")
        print(f"gs startup failed:\n{err}")
        dutil.compare_dev_shm(shm_status)
        return LAUNCHER_FAIL_EXIT

    try:
        exit_code = output_monitor(la_in_rh)
    except KeyboardInterrupt:
        print("KeyboardInterrupt - going to teardown")
        log.warning("KeyboardInterrupt - tearing down")
        exit_code = LAUNCHER_FAIL_EXIT

    try:
        gs_in_wh.send(dmsg.GSTeardown(tag=dlutil.next_tag()).serialize())
        gs_halt = shutdown_monitor(la_in_rh)
        time.sleep(TIMEOUT_YIELD)
        ls_in_wh.send(dmsg.SHTeardown(tag=dlutil.next_tag()).serialize())
        be_halt = shutdown_monitor(la_in_rh)
        time.sleep(TIMEOUT_YIELD)
        ls_stdin.send(dmsg.BEHalted(tag=dlutil.next_tag()).serialize())
        sh_halt = dmsg.parse(ls_stdout.recv())

        if not isinstance(gs_halt, dmsg.GSHalted):
            log.warning(f"expected GSHalted got {gs_halt}")

        if not isinstance(be_halt, dmsg.SHHaltBE):
            log.warning(f"expected SHHaltBE got {be_halt}")

        if not isinstance(sh_halt, dmsg.SHHalted):
            log.warning(f"expected SHHalted got {sh_halt}")
    except TimeoutError:
        log.exception("teardown error")

    try:
        gs_thread.join(timeout=TIMEOUT_PATIENCE)
        ls_thread.join(timeout=TIMEOUT_PATIENCE)
    except TimeoutError:
        log.warning("infrastructure thread hang")
        print("warning: infrastructure thread hang")

    dutil.compare_dev_shm(shm_status)

    return exit_code


if __name__ == "__main__":
    ecode = main()
    sys.exit(ecode)
