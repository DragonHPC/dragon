#!/usr/bin/env python3

import dragon.channels as dch
import dragon.managed_memory as dmm
import dragon.infrastructure.connection as dic

pool_name = "hyenas"
pool_size = 2 ** 30
pool_prealloc_blocks = None
pool_uid = 17
print('pool creating')
mpool = dmm.MemoryPool(pool_size, pool_name, pool_uid, pool_prealloc_blocks)
print('pool created')

channel_uid = 42
chan = dch.Channel(mpool, channel_uid)
print('channel created')

writer = dic.Connection(outbound_initializer==chan)
reader = dic.Connection(inbound_initializer=chan)

print('reading and writing connections made')

thing_to_send = 'hyena hyena hyena hyena'

writer.send(thing_to_send)
thing_got = reader.recv()

if thing_to_send == thing_got:
    print('hyenas received ok')
else:
    print('got {} and not {}'.format(thing_got, thing_to_send))

writer.close()

try:
    end_thing = reader.recv()
except EOFError as err:
    print('got the EOF')

reader.close()

chan.destroy()
mpool.destroy()
