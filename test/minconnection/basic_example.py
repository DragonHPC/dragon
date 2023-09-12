import dragon.infrastructure.minconnection as dimc
import dragon.channels as dch
import dragon.managed_memory as dmm

# a basic example of how to use MinConnection

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

reading_mc = dimc.MinConnection(mpool, chan, True)
writing_mc = dimc.MinConnection(mpool, chan, False)

sendstr = 'hyenas!'
writing_mc.send(sendstr)
gotstr = reading_mc.recv()
assert (sendstr.strip() == gotstr.strip())

print('polling empty')
pollres = reading_mc.poll(2)
print(f'poll res is {pollres}')

writing_mc.send(sendstr)
print('polling nonempty')
pollres = reading_mc.poll(2)
print(f'poll res is {pollres}')
print('polling nonempty again')
pollres = reading_mc.poll(2)
print(f'poll res is {pollres}')
print('reading')
anotherstr = reading_mc.recv()
assert (sendstr.strip() == anotherstr.strip())

# this will hang, but it shows
# the default queue depth
# for i in range(1500):
#     print(f'pushing {i}')
#     writing_mc.send(sendstr)


# this closes the handles, but the
# underlying channel object is still there
# the connection object does not take ownership
# of this.
reading_mc.close()
writing_mc.close()

chan.destroy()
print('channel destroyed')

mpool.destroy()
print('pool destroyed')
print('goodbye')
