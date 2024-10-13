#!/usr/bin/env python3

"""An ultra simple way to start the infrastructure (LS & GS) and
   start a script under it.  The script can do what it likes.
"""
import atexit
import logging
import threading
import multiprocessing
import os
import pickle
import subprocess
import sys
import traceback

import dragon.channels as dch
import dragon.managed_memory as dmm

import dragon.globalservices.api_setup as dapi
import dragon.globalservices.process as dproc
import dragon.globalservices.process_desc as pdesc
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.log_setup as log_setup
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.parameters as dparm
import dragon.localservices.local_svc as dsls
import dragon.utils as du
import support.util as tsu


def start_ls(shep_stdin_queue, shep_stdout_queue, env_update, name_addition=''):
    log_setup.setup_logging(basename='ls_' + name_addition, the_level=logging.DEBUG)
    log = logging.getLogger('ls-gs integration test channels, ls starts gs')
    log.info('--------------------ls started-----------------------')

    dparm.this_process.mode = dfacts.SINGLE_MODE
    os.environ.update(dparm.this_process.env())
    log.info('starting')
    try:
        dsls.single(test_stdin=shep_stdin_queue, test_stdout=shep_stdout_queue,
                    gs_args=[sys.executable, '-c', dfacts.GS_SINGLE_LAUNCH_CMD],
                    gs_env=env_update)
    except RuntimeError as rte:
        log.exception('runtime failure\n')
        shep_stdout_queue.send(dmsg.AbnormalTermination(tag=dsls.get_new_tag(),
                                                        err_info=f'{rte!s}').serialize())
    log.info('exited')


class Context:
    def __init__(self):
        self.la_tag_cnt = 0
        self.mode = dfacts.SINGLE_MODE

        self.shep_stdin_rh, self.shep_stdin_wh = multiprocessing.Pipe(duplex=False)
        self.shep_stdout_rh, self.shep_stdout_wh = multiprocessing.Pipe(duplex=False)

        username = subprocess.check_output('whoami').decode().strip()
        self.pool_name = 'standalone_' + username
        self.pool_size = 2 ** 30
        self.pool_uid = dfacts.FIRST_MUID + 2 ** 20
        self.test_pool = dmm.MemoryPool(self.pool_size, self.pool_name, self.pool_uid)

        # will need to make a 'test pool' and 'test channels' and pass
        # those to GS thru environment, by means of putting them into the shepherd dut's
        # own startup environment, then inherited.

        # these channels are ones that normally would be in the infrastructure
        # pool; they are used by the testbench to let it call the gs API
        # and get answers back in the usual way.

        # faking these up and making them really large so they won't be likely to
        # clash with anything assigned by GS in a test
        self.gs_return_cuid = 2 ** 64 - 17
        self.proc_gs_return_chan = dch.Channel(self.test_pool, self.gs_return_cuid)
        self.proc_gs_return_rh = dconn.Connection(inbound_initializer=self.proc_gs_return_chan)

        # not used in this test, but just for uniform practice.
        self.shep_return_cuid = 2 ** 64 - 18
        self.proc_shep_return_chan = dch.Channel(self.test_pool, self.shep_return_cuid)
        self.proc_shep_return_rh = dconn.Connection(inbound_initializer=self.proc_shep_return_chan)

        self.shep_dut = None
        self.inf_pool = None
        self.shep_input_chan = None
        self.shep_input_wh = None
        self.bela_input_chan = None
        self.bela_input_rh = None
        self.gs_input_chan = None
        self.gs_input_wh = None
        self.head_puid = None

    def start_infrastructure(self):
        log_name_addition = subprocess.check_output('whoami').decode().strip()

        # Make a list of channels to pass thru the environment to GS for testing purposes.
        # In reality these channels would be created by GS in advance of starting
        # the infrastructure-using process.
        test_chan_list = [(self.gs_return_cuid, self.test_pool.serialize(),
                           self.proc_gs_return_chan.serialize(), False),
                          (self.shep_return_cuid, self.test_pool.serialize(),
                           self.proc_shep_return_chan.serialize(), False)]

        val = pickle.dumps(test_chan_list)
        env_update = {dfacts.GS_TEST_CH_EV: du.B64.bytes_to_str(val),
                      dfacts.GS_LOG_BASE: 'gs_' + log_name_addition}

        shep_start_args = (self.shep_stdin_rh, self.shep_stdout_wh, env_update)

        self.shep_dut = multiprocessing.Process(target=start_ls,
                                                args=shep_start_args,
                                                kwargs={'name_addition': log_name_addition})

        self.shep_dut.start()

        # start shepherd startup protocol for single node
        self.shep_stdin_wh.send(dmsg.BENodeIdxSH(tag=self.la_tag_cnt, node_idx=0, net_conf_key="0").serialize())
        self.la_tag_cnt += 1

        ping_be_msg = tsu.get_and_check_type(self.shep_stdout_rh, dmsg.SHPingBE)

        env2b = du.B64.str_to_bytes

        self.inf_pool = dmm.MemoryPool.attach(env2b(ping_be_msg.inf_pd))

        self.shep_input_chan = dch.Channel.attach(env2b(ping_be_msg.shep_cd))
        self.shep_input_wh = dconn.Connection(outbound_initializer=self.shep_input_chan)

        self.bela_input_chan = dch.Channel.attach(env2b(ping_be_msg.be_cd))
        self.bela_input_rh = dconn.Connection(inbound_initializer=self.bela_input_chan)

        self.gs_input_chan = dch.Channel.attach(env2b(ping_be_msg.gs_cd))
        self.gs_input_wh = dconn.Connection(outbound_initializer=self.gs_input_chan)

        # test bench is acting as though it is the GS api, and the normal module import
        # initialization won't have triggered, so we override the connections so the API
        # will work.

        dapi.test_connection_override(test_gs_input=self.gs_input_wh,
                                      test_gs_return=self.proc_gs_return_rh,
                                      test_gs_return_cuid=self.gs_return_cuid,
                                      test_shep_input=self.shep_input_wh,
                                      test_shep_return=self.proc_shep_return_rh,
                                      test_shep_return_cuid=self.shep_return_cuid)

        # complete shepherd startup.
        self.shep_input_wh.send(dmsg.BEPingSH(tag=self.la_tag_cnt).serialize())
        self.la_tag_cnt += 1

        tsu.get_and_check_type(self.bela_input_rh, dmsg.SHChannelsUp)

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSIsUp)

    def cleanup(self):
        self.gs_input_wh.send(dmsg.GSTeardown(tag=self.la_tag_cnt).serialize())
        self.la_tag_cnt += 1

        tsu.get_and_check_type(self.bela_input_rh, dmsg.GSHalted)

        self.shep_input_wh.send(dmsg.SHTeardown(tag=self.la_tag_cnt).serialize())
        self.la_tag_cnt += 1

        tsu.get_and_check_type(self.bela_input_rh, dmsg.SHHaltBE)

        self.shep_stdin_wh.send(dmsg.BEHalted(tag=self.la_tag_cnt).serialize())
        self.la_tag_cnt += 1

        tsu.get_and_check_type(self.shep_stdout_rh, dmsg.SHHalted)

        self.shep_stdin_rh.close()
        self.shep_stdin_wh.close()
        self.shep_stdout_rh.close()
        self.shep_stdout_wh.close()

        try:
            self.shep_input_wh.close()
        except:
            pass

        try:
            self.bela_input_rh.close()
        except:
            pass

        try:
            self.gs_input_wh.close()
        except:
            pass

        try:
            self.proc_gs_return_rh.close()
        except:
            pass

        try:
            self.proc_shep_return_rh.close()
        except:
            pass

        try:
            self.shep_input_chan.detach()
        except:
            pass

        try:
            self.bela_input_chan.detach()
        except:
            pass

        try:
            self.gs_input_chan.detach()
        except:
            pass

        try:
            self.proc_gs_return_chan.destroy()
        except:
            pass

        try:
            self.proc_shep_return_chan.destroy()
        except:
            pass

        try:
            self.inf_pool.detach()
        except:
            pass

        try:
            self.test_pool.destroy()
        except:
            pass

        try:
            self.shep_dut.join(timeout=1)
        except TimeoutError:
            self.shep_dut.kill()

    def start_thing(self, start_args):
        res = dproc.create(exe='python3', run_dir='',
                           args=start_args, env={},
                           options=pdesc.ProcessOptions(make_inf_channels=True))

        self.head_puid = res.p_uid

    def monitor_output(self, optional_barrier=None):
        while True:
            msg = dmsg.parse(self.bela_input_rh.recv())
            if isinstance(msg, dmsg.SHFwdOutput):
                if msg.fd_num == msg.FDNum.STDOUT.value:
                    sys.stdout.write(msg.data)
                    sys.stdout.flush()
                elif msg.fd_num == msg.FDNum.STDERR.value:
                    sys.stderr.write(msg.data)
                    sys.stderr.flush()
                if optional_barrier is not None:
                    try:
                        optional_barrier.wait()
                    except threading.BrokenBarrierError:
                        pass
            elif isinstance(msg, dmsg.GSHeadExit):
                print('+++ head proc exited, code {}'.format(msg.exit_code))
                break
            else:
                print('unexpected message: {}'.format(msg))


default_parent_stdin_filename = f'quokka_{os.getpid()}'
parent_raw_input_fh = None
coordinate_repl_prompt_barrier = threading.Barrier(2)


def get_raw_input_from_parent_via_fd(prompt='D>>'):
    'Intended to be called as substitute for raw_input() in child process.'
    global parent_raw_input_fh, default_parent_stdin_filename
    if parent_raw_input_fh is None:
        print(f'DBG {default_parent_stdin_filename=}', flush=True)
        parent_raw_input_fh = open(default_parent_stdin_filename, 'rt')
    sys.stdout.flush()
    input_text = parent_raw_input_fh.readline()
    return input_text.rstrip()


def capture_raw_input_as_parent_via_fd(
        filename=default_parent_stdin_filename,
        barrier=coordinate_repl_prompt_barrier
):
    os.mkfifo(filename)
    barrier.wait()
    print(f'DBG parent process {os.getpid()=}')
    try:
        with open(filename, 'wt') as parent_raw_input_fh:
            while True:
                print('D>> ', end='', flush=True)
                input_text = sys.stdin.readline()
                parent_raw_input_fh.write(input_text)
                parent_raw_input_fh.flush()
                try:
                    barrier.wait(timeout=1)
                except threading.BrokenBarrierError:
                    pass
    except Exception as e:
        print(f'Capture ending, reason: {e}', flush=True)


def start_repl(pipe_filename):
    import code
    g = globals()
    g['default_parent_stdin_filename'] = pipe_filename
    code.interact(readfunc=get_raw_input_from_parent_via_fd, local=g)


def main(start_args, interactive=False):
    the_ctx = Context()
    try:
        the_ctx.start_infrastructure()
    except Exception as e:
        print('Got exception {} in inf startup'.format(e))
        traceback.print_exc()
        the_ctx.cleanup()
        exit(1)

    if interactive:
        capture_raw_input_thread = threading.Thread(
            target=capture_raw_input_as_parent_via_fd,
            args=(default_parent_stdin_filename, coordinate_repl_prompt_barrier)
        )
        capture_raw_input_thread.start()

    try:
        the_ctx.start_thing(start_args)
    except Exception as e:
        print('Got exception {} in program start'.format(e))
        traceback.print_exc()
        the_ctx.cleanup()
        exit(1)

    # try this to handle ctl-C while stuff runs.
    atexit.register(the_ctx.cleanup)

    if interactive:
        monitor_output_thread = threading.Thread(
            target=Context.monitor_output,
            args=(the_ctx, coordinate_repl_prompt_barrier)
        )
        monitor_output_thread.start()
        monitor_output_thread.join()
    else:
        the_ctx.monitor_output()

    atexit.unregister(the_ctx.cleanup)
    the_ctx.cleanup()

    try:
        # Can not be done in start_repl() because still in use at that time.
        os.unlink(default_parent_stdin_filename)
    except:
        pass

    exit(0)


if __name__ == "__main__":
    if 1 == len(sys.argv):
        args = [
            '-c',
            f'import start_single; start_single.start_repl({default_parent_stdin_filename!r})'
        ]
        interactive = True
    else:
        args = sys.argv[1:]
        interactive = False

    main(args, interactive)
