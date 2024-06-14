#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dragon/channels.h>
#include <dragon/utils.h>

#include "ch_p2p_common.h"

dragonError_t
attach_to_channel(char *b64_channel_data, dragonChannelDescr_t *pdragon_channel_descr)
{
    dragonChannelSerial_t dragon_channel_serial;

    DEBUG_PRINT(("Decoding channels's serialized descriptor\n"));
    dragon_channel_serial.data = dragon_base64_decode(
        b64_channel_data,
        strlen(b64_channel_data),
        &dragon_channel_serial.len);

    DEBUG_PRINT(("Attaching to channel %s\n", b64_channel_data));
    return dragon_channel_attach(&dragon_channel_serial, pdragon_channel_descr);
}

dragonError_t
attach_to_memory_pool(char *b64_mpool_data, dragonMemoryPoolDescr_t *pdragon_mpool_descr)
{
    dragonMemoryPoolSerial_t dragon_mpool_serial;

    DEBUG_PRINT(("Decoding memory pool's serialized descriptor\n"));
    dragon_mpool_serial.data = dragon_base64_decode(
        b64_mpool_data,
        &dragon_mpool_serial.len);

    DEBUG_PRINT(("Attaching to memory pool\n"));
    return dragon_memory_pool_attach(pdragon_mpool_descr, &dragon_mpool_serial);
}

dragonError_t
setup(int num_channels, char *channel_descr[],
      int node_id, char *default_mpool_descr,
      dragonChannelDescr_t dragon_channels[],
      dragonMemoryPoolDescr_t *dragon_default_mpool,
      dragonChannelSendh_t *dragon_ch_send_handle,
      dragonChannelRecvh_t *dragon_ch_recv_handle)
{
    dragonError_t derr;

    dragon_channel_register_gateways_from_env();

    for (int idx = 0; idx < num_channels; idx++) {
        DEBUG_PRINT(("Attaching to channel %i\n", idx));
        derr = attach_to_channel(channel_descr[idx], &dragon_channels[idx]);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Failed to attach to channel");
        }

        if (idx == node_id) {
            DEBUG_PRINT(("Getting channel %i's send handle\n", idx));
            derr = dragon_channel_sendh(&dragon_channels[idx], dragon_ch_send_handle, NULL);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Failed to get channel send handle for channel");
            }

            DEBUG_PRINT(("Opening channel %i's send handle\n", idx));
            derr = dragon_chsend_open(dragon_ch_send_handle);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Failed to open channel send handle for channel");
            }
        } else {
            DEBUG_PRINT(("Getting channel %i's recv handle\n", idx));
            derr = dragon_channel_recvh(&dragon_channels[idx], dragon_ch_recv_handle, NULL);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Failed to get channel recv handle for channel");
            }

            DEBUG_PRINT(("Opening channel %i's recv handle\n", idx));
            derr = dragon_chrecv_open(dragon_ch_recv_handle);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Failed to open channel recv handle for channel");
            }
        }
    }

    DEBUG_PRINT(("Attaching to default memory pool\n"));
    derr = attach_to_memory_pool(default_mpool_descr, dragon_default_mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to attach to default memory pool");
    }

    return DRAGON_SUCCESS;
}

dragonError_t
cleanup(int num_channels, int node_id,
        dragonChannelDescr_t dragon_channels[],
        dragonMemoryPoolDescr_t *dragon_default_mpool,
        dragonChannelSendh_t *dragon_ch_send_handle,
        dragonChannelRecvh_t *dragon_ch_recv_handle)
{
    dragonError_t derr;

    DEBUG_PRINT(("Detaching from default memory pool\n"));
    derr = dragon_memory_pool_detach(dragon_default_mpool);
    if (derr != DRAGON_SUCCESS) {
        err_fail(derr, "Failed to detach from default memory pool");
    }

    for (int idx = 0; idx < num_channels; idx++) {
        if (idx == node_id) {
            DEBUG_PRINT(("Closing channel %i's send handle\n", idx));
            derr = dragon_chsend_close(dragon_ch_send_handle);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Failed to close send handle");
            }
        } else {
            DEBUG_PRINT(("Closing channel %i's recv handle\n", idx));
            derr = dragon_chrecv_close(dragon_ch_recv_handle);
            if (derr != DRAGON_SUCCESS) {
                err_fail(derr, "Failed to close recv handle");
            }
        }

        DEBUG_PRINT(("Detaching from channel %i\n", idx));
        derr = dragon_channel_detach(&dragon_channels[idx]);
        if (derr != DRAGON_SUCCESS) {
            err_fail(derr, "Failed to detach from channel");
        }
    }

    return DRAGON_SUCCESS;
}
