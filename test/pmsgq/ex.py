from dragon.launcher.pmsgqueue import PMsgQueue
import dragon.infrastructure.messages as dmsg
import os

pid = os.getpid()

sendh = PMsgQueue("/test" + str(pid), write_intent=True)
recvh = PMsgQueue("/test" + str(pid), read_intent=True)
sendh.send("Bad Message")
sendh.reset()
recvh.reset()

sendh.send(dmsg.SHPingBE(tag=0, shep_cd="", be_cd="", gs_cd="", default_pd="", inf_pd="").serialize())

msg = recvh.recv()
msg = dmsg.parse(msg)
print(repr(msg))


recvh.close()
sendh.close(destroy=True)
