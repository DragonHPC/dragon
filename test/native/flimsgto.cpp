#include <dragon/messages.hpp>
#include <dragon/fli.h>
#include <dragon/utils.h>
#include <iostream>

using namespace std;

int main(int argc, char* argv[]) {
    try {
        dragonError_t err;
        dragonFLIDescr_t fli;
        dragonFLIRecvHandleDescr_t recvh;
        dragonFLISerial_t ser_fli;
        DragonMsg* msg;

        if (argc != 2) {
            cout << "FAILED: This program expects a serialized fli as its command-line argument." << endl;
            return -1;
        }

        const char* serFLIb64 = argv[1];
        ser_fli.data = dragon_base64_decode(serFLIb64, &ser_fli.len);

        err = dragon_fli_attach(&ser_fli, NULL, &fli);
        if (err != DRAGON_SUCCESS) {
            cout << "FAILED to attach to FLI" << endl;
            cerr << "FAILED to attach to FLI" << endl;
            return -1;
        }

        err = dragon_fli_open_recv_handle(&fli, &recvh, NULL, NULL, NULL);
        if (err != DRAGON_SUCCESS) {
            cout << "Failed to open recv handle" << endl;
            cerr << "Failed to open recv handle" << endl;
            return -1;
        }

        err = recv_fli_msg(&recvh, &msg, NULL);
        if (err != DRAGON_SUCCESS) {
            cout << "Failed to recv msg" << endl;
            cerr << "Failed to recv msg" << endl;
            return -1;
        }

        if (msg->tc() != DD_REGISTER_CLIENT) {
            cout << "Failed to match typecode." << endl;
            cerr << "Failed to match typecode." << endl;
            return -1;
        }

        DDRegisterClientMsg* rc_msg = (DDRegisterClientMsg*) msg;

        if (strcmp(rc_msg->respFLI(), "Hello World!")) {
            cout << "Failed to find expected string in message" << endl;
            cerr << "Failed to find expected string in message" << endl;
            return -1;
        }

        cout << "OK" << endl;
        return 0;
    } catch (...) {
        cerr << "Exception in code." << endl << flush;
        cout << "Exception in code." << endl << flush;
    }

    return -1;
}