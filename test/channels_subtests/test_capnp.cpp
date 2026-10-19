#include <dragon/message_defs.capnp.h>
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <iostream>

// ./test_capnp > test.out
// capnp decode --packed ../../src/lib/message_defs.capnp DragonMessage < test.out
// Running this should print:
// ( header = (tc = 6, tag = 4, ref = 1, err = 0),
//  registerClient = (test = "Hello World!") )

int main(int argc, char* argv[]) {
    capnp::MallocMessageBuilder message;

    MessageDef::Builder msg = message.initRoot<MessageDef>();
    msg.setTc(6);
    msg.setTag(4);
    DDRegisterClientDef::Builder rc = msg.initDdRegisterClient();
    rc.setRespFLI("Hello World!");
    capnp::writeMessageToFd(1, message);
    return 0;
}