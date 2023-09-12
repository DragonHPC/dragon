#!/usr/bin/env python3

# This is a target program for a test of the Channels-based
# Connection object and argument delivery as needed
# for milestone 1.  It tests integration of the global
# services API, global services, and the shepherd in being
# able to use the API to ask GS for existing channels,
# communicate through them, use the Pipe constructor to
# make connection objects anonymously

# It starts as a managed process and looks at the delivered
# arguments for the name of input and output channels which the
# testbench should have already created.  It will query
# GS for those channels, make Connection objects out of them,
# then use the Pipe constructor anonymously to make two more
# Connection objects. It'll send the reading end of the new
# Connection through the original output Connection made
# from the pre-existing channel and expect to get another Connection
# through the original input Connection.  Then through these
# new ones it will send some data.

# This basically proves that we can get Channels by name, make Connections
# out of them, construct Connections anonymously, and serialize/deserialize
# them after construction and send data thru them.

# The test bench calling this script will start two mirror image copies.
# The script takes only one command line argument which is mainly used
# to differentiate the log files produced by either copy

import sys
import logging
import pickle

import dragon.channels as dch

import dragon.globalservices.api_setup as dapi
import dragon.globalservices.channel
import dragon.infrastructure.connection as dconn
import dragon.infrastructure.parameters as dparm
import dragon.dlogging.util as dlog

if len(sys.argv) < 2:
    exit(1)

identity = sys.argv[1]

dlog.setup_logging(basename='catt-' + identity, level=logging.DEBUG)
log = logging.getLogger('')
log.info('hello from conn test worker {}'.format(identity))
log.info('p_uid is {}'.format(dparm.this_process.my_puid))

try:
    dapi.connect_to_infrastructure()
except Exception as err:
    log.info('failed to attach to infrastructure')
    log.exception(f'error is {err}')
    exit(1)

log.info('connected to infrastructure')

# now look at args to get inbound and outbound channel *names*
my_args = pickle.loads(dapi._ARG_PAYLOAD)

log.info(f'my args: {my_args}')

if len(my_args) != 2:
    log.error('unexpected number of args: {}'.format(len(my_args)))
    exit(1)

inbound_cname, outbound_cname = my_args

# ask for the descriptors for these args
try:
    inbound_cdesc = dragon.globalservices.channel.query(inbound_cname)
    outbound_cdesc = dragon.globalservices.channel.query(outbound_cname)
except Exception as e:
    log.exception("couldn't query existing channels")
    exit(1)

log.info('queried pre-existing channels')

try:
    inbound_ch = dch.Channel.attach(inbound_cdesc.sdesc)
    outbound_ch = dch.Channel.attach(outbound_cdesc.sdesc)
except Exception as e:
    log.exception("couldn't attach to existing channels")
    exit(1)

log.info('attached to pre-existing channels')

# should try this with a full duplex Connection object
orig_inbound_con = dconn.Connection(inbound_initializer=inbound_ch)
orig_outbound_con = dconn.Connection(outbound_initializer=outbound_ch)

first_msg = 'weasels'

try:
    orig_outbound_con.send(first_msg)
    partner = orig_inbound_con.recv()
except Exception as e:
    log.exception("couldn't send/recv initial msg", exc_info=True)
    exit(1)

if first_msg != partner:
    log.error('exchanged msgs do not match: {} vs {}'.format(first_msg, partner))
    exit(1)

log.info('we got our {}, now for a pipe and connection'.format(first_msg))

my_reader, my_writer = dconn.Pipe(duplex=False)

log.info('pipe called')

# should the process of sending my_reader put it in a
# state where it shouldn't be opened?
orig_outbound_con.send(my_reader)

log.info('reader obj sent')

others_reader = orig_inbound_con.recv()
log.info('reader obj received')

# need to do this, otherwise there will be
# a deadlock.
others_reader.open()
log.info('partner reader opened')

my_message = 'hyenas'

try:
    my_writer.send(my_message)
    log.info('my message is sent')
except Exception as e:
    log.exception("Couldn't send second message thru Connection")
    exit(1)

try:
    others_message = others_reader.recv()
    log.info('got the message from the other side')
except:
    log.exception("Couldn't get second message thru Connection")
    exit(1)

if my_message == others_message:
    log.info('messages are fine')
else:
    log.info('mismatch: {} vs {}'.format(my_message, others_message))
    exit(1)
# todo: some stuff here to do the cleanup explicitly

exit(0)
