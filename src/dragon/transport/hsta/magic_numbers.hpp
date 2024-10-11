#ifndef MAGIC_NUMBERS_HPP
#define MAGIC_NUMBERS_HPP

#define HSTA_DEBUG_MAX_LEVEL 2

#define HSTA_NUM_GW_TYPES 3
#define HSTA_DEFAULT_GW_ENV_PREFIX "DRAGON_HSTA_GW"

#define HSTA_MAIN_THREAD_IDX -1

#define HSTA_CAT_NAP_TIME                  1
#define HSTA_WAIT_TIME_FOR_DEBUGGER_ATTACH 5
#define HSTA_QUIESCE_TIMEOUT               ((double)1.0)

// TODO: cast these to correct types
#define HSTA_INVALID_INDEX -1
#define HSTA_INVALID_RANK  -2
#define HSTA_INVALID_NID   -3
#define HSTA_INVALID_PORT  -4
#define HSTA_INVALID_SIZE  -5

#define HSTA_INVALID_TIME   0xffffffffffffffffUL
#define HSTA_INVALID_UINT64 0xffffffffffffffffUL
#define HSTA_INVALID_INT64  0xffffffffffffffffL
#define HSTA_INVALID_UINT32 0xffffffffU
#define HSTA_INVALID_INT32  ((int32_t)0xffffffff)

#define HSTA_TESTING_MODE_TAG_NBITS  8
#define HSTA_TESTING_MODE_MAX_TAG    ((1 << HSTA_TESTING_MODE_TAG_NBITS) - 1)
// 2**15 == 32768 == minimal max-tag-value required by MPI
#define HSTA_TESTING_MODE_MAX_NID    ((1 << (15 - HSTA_TESTING_MODE_TAG_NBITS)) - 1)

#define HSTA_MAX_BITS_NID  16
#define HSTA_MAX_BITS_PORT 16

#define HSTA_MAX_NUM_NICS         16
#define HSTA_MAX_PACKED_RKEY_SIZE 64

#define HSTA_TX_QUEUE_SIZE         8UL
#define HSTA_RX_QUEUE_FACTOR       2UL
#define HSTA_RX_QUEUE_SIZE_MAX     128UL
// we currently have only one aggregated rx cq with index 0
#define HSTA_AG_RX_CQ_IDX          0
#define HSTA_DEFAULT_NUM_RX_QUEUES 1

// reserve 64 bytes for the AgHeader and just a little extra padding
#define HSTA_RNDV_THRESHOLD                  (HSTA_EAGER_BUF_MAX_BYTES - 64)
#define HSTA_DEFAULT_WORK_STEALING_THRESHOLD (1UL << 17)

#define HSTA_MAX_EP_ADDR_LEN 4096

#define HSTA_NBITS_IN_BYTE 8
#define HSTA_NBYTES_IN_MB  (1UL << 20)

#define HSTA_DEFAULT_FID      0xffffffffffffffff
#define HSTA_DEFAULT_SEQNUM   0xffffffffffffffff
#define HSTA_DEFAULT_CHECKSUM 0xffffffffffffffff

#define HSTA_FNV1_SHIFT_0 1
#define HSTA_FNV1_SHIFT_1 4
#define HSTA_FNV1_SHIFT_2 5
#define HSTA_FNV1_SHIFT_3 7
#define HSTA_FNV1_SHIFT_4 8
#define HSTA_FNV1_SHIFT_5 40

// 64KB for max eager size
#define HSTA_EAGER_SIZE_MAX           0x10000
// 2048 bytes for an eager buffer
#define HSTA_EAGER_BUF_MAX_BYTES      0x800
// cacheline-aligned buffers by default
#define HSTA_DEFAULT_BUFFER_ALIGNMENT 0x40

// 8MB for the maximum number of bytes currently being received over the network
#define HSTA_DEFAULT_MAX_EJECTION_MB 8UL
// 8MB for the maximum number of bytes currently being received over a local channel
#define HSTA_DEFAULT_MAX_GETMSG_MB   8UL

// TODO: do we need a fill count? we already purge the eager buffer
// once it fill up, so spin count should be what we focus on.

// number of items to add to eager buffer before purging it
#define HSTA_EAGER_BUF_FILL_COUNT_MIN 128
// number of spins through progress loop before puring eager buffer
#define HSTA_EAGER_BUF_SPIN_COUNT_MIN 2

#define HEADERTYPE_SHIFT  5
#define SEND_RETURN_SHIFT 2
#define HEADERTYPE_MASK   ((uint8_t) 0xe0) // 6th, 7th and 8th bits
#define SEND_RETURN_MASK  ((uint8_t) 0x1c) // 3rd, 4th and 5th bits
#define PROTOCOLTYPE_MASK ((uint8_t) 0x03) // first 2 bits

#define TIMEOUT_SHIFT 2
#define TIMEOUT_MASK  0x3

#define HSTA_HINTS_SHIFT   1
#define HSTA_CLIENTID_MASK ((uint8_t) 0x01)
#define HSTA_HINTS_MASK    ((uint8_t) 0x02)

#define HSTA_INVALID_CLIENTID_HINTS 0ul

#define EVENT_MASK_NULL 0x0

#define ALL_ONES_64 0xffffffffffffffff

#endif // MAGIC_NUMBERS_HPP
