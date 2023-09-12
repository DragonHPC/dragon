import dragon.localservices.local_svc as dsls
import dragon.infrastructure.log_setup as dlog
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as dfacts
import logging
import os
import threading

dlog.setup_logging(basename='try_ls', the_level=logging.DEBUG)
log = logging.getLogger('driver')

print(f'{os.getpid()=}, {os.getpgid(0)=}')
os.setpgid(0, 0)
msg = f'now {os.getpgid(0)=}'
print(msg)
log.info(msg)

ls = dsls.LocalServer()

dying = dmsg.SHProcessCreate(tag=0, p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID,
                             t_p_uid=99, exe='/bin/sleep', args=['2'])

ls.create_process(dying)
print('created dying process')

channel_id, msg = ls.responses.get(timeout=1)
assert isinstance(msg, dmsg.SHProcessCreateResponse)
proc = ls.new_procs.get(timeout=1)

print(f'got response: {proc.pid=}')
print(f'{channel_id=}, {msg=!r}')

death = threading.Thread(target=ls.watch_death)
death.start()

print('waiting for death')
channel_id, msg = ls.responses.get(timeout=3)
assert isinstance(msg, dmsg.SHProcessExit)
print(f'{channel_id=}, {msg=!r}')

print('killing death')
ls.set_shutdown('test')
death.join(timeout=1)
print('victory over death')

# now one that exercises some i/o.

ls.shutdown_sig.clear()

chatty = dmsg.SHProcessCreate(tag=1, p_uid=dfacts.GS_PUID, r_c_uid=dfacts.GS_INPUT_CUID,
                              t_p_uid=100, exe='/bin/echo', args=['hyenas'])

iowatcher = threading.Thread(target=ls.watch_output)
iowatcher.start()
ls.create_process(chatty)

print('chatty process created')

channel_id, msg = ls.responses.get(timeout=1)
print(f'{channel_id=}, {msg=!r}')
channel_id, msg = ls.responses.get(timeout=1)
print(f'{channel_id=}, {msg=!r}')

ls.set_shutdown('test 2')
iowatcher.join(timeout=1)
print('iowatcher left well')
