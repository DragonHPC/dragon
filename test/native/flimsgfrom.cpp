#include <dragon/messages.hpp>
#include <dragon/fli.h>
#include <dragon/utils.h>
#include <iostream>

using namespace std;

int main(int argc, char* argv[]) {
    try {
        dragonError_t err;
        dragonFLIDescr_t fli;
        dragonFLISendHandleDescr_t sendh;
        dragonFLISerial_t ser_fli;

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

        err = dragon_fli_open_send_handle(&fli, &sendh, NULL, NULL, false, true, NULL);
        if (err != DRAGON_SUCCESS) {
            cout << "Failed to open send handle" << endl;
            cerr << "Failed to open send handle" << endl;
            return -1;
        }

        DDRegisterClientMsg msg(42, "Hello World", "Dragon is the best");

        msg.send(&sendh, NULL);

        cout << "OK" << endl;
        return 0;
    } catch (...) {
        cerr << "Exception in code." << endl << flush;
        cout << "Exception in code." << endl << flush;
    }

    return -1;
}