#ifndef CH_P2P_COMMON_H
#define CH_P2P_COMMON_H

#include <dragon/return_codes.h>

#define SUCCESS 0
#define FAILED 1

#ifdef DEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif

#define err_fail(derr, msg) ({                                                                                        \
    char *errstr = dragon_getlasterrstr();                                                                            \
    const char *errcode = dragon_get_rc_string(derr);                                                                 \
    printf("TEST: %s:%d | %s.  Got EC=%s (%i)\nERRSTR = \n%s\n", __FUNCTION__, __LINE__, msg, errcode, derr, errstr); \
    return FAILED;                                                                                                    \
})

#define main_err_fail(derr, msg, jmp) ({                                                                                   \
    char *errstr = dragon_getlasterrstr();                                                                                 \
    const char *errcode = dragon_get_rc_string(derr);                                                                      \
    printf("TEST_MAIN: %s:%d | %s.  Got EC=%s (%i)\nERRSTR = \n%s\n", __FUNCTION__, __LINE__, msg, errcode, derr, errstr); \
    TEST_STATUS = FAILED;                                                                                                  \
    goto jmp;                                                                                                              \
})

#ifdef __cplusplus
extern "C" {
#endif

dragonError_t
setup(int num_channels, char *channel_descr[],
        int node_id, char *default_mpool_descr,
        dragonChannelDescr_t dragon_channels[],
        dragonMemoryPoolDescr_t *dragon_default_mpool,
        dragonChannelSendh_t *dragon_ch_send_handle,
        dragonChannelRecvh_t *dragon_ch_recv_handle);

dragonError_t
cleanup(int num_channels, int node_id,
        dragonChannelDescr_t dragon_channels[],
        dragonMemoryPoolDescr_t *dragon_default_mpool,
        dragonChannelSendh_t *dragon_ch_send_handle,
        dragonChannelRecvh_t *dragon_ch_recv_handle);

#ifdef __cplusplus
}
#endif

#endif