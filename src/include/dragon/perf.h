/*
 * Copyright 2023 Hewlett Packard Enterprise Development LP
 */

#ifndef DRAGON_PERF_H
#define DRAGON_PERF_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum dragonChPerfOpcode {
    DRAGON_PERF_OPCODE_SEND_MSG = 0,
    DRAGON_PERF_OPCODE_GET_MSG,
    DRAGON_PERF_OPCODE_PEEK,
    DRAGON_PERF_OPCODE_POP,
    DRAGON_PERF_OPCODE_POLL,
    DRAGON_PERF_OPCODE_LAST
} dragonChPerfOpcode_t;

dragonError_t dragon_chperf_session_new(dragonChannelSerial_t *sdesc_array, int num_channels);
dragonError_t dragon_chperf_session_cleanup();
dragonError_t dragon_chperf_kernel_new(int kernel_idx, int ch_idx, int num_procs);
dragonError_t dragon_chperf_kernel_append_op(int kernel_idx, dragonChPerfOpcode_t op_code, int dst_ch_idx, size_t size_in_bytes, double timeout_in_sec);
dragonError_t dragon_chperf_kernel_run(int kernel_idx, double *run_time);

#ifdef __cplusplus
}
#endif

#endif // DRAGON_PERF_H

