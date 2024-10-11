#ifndef HEADER_HPP
#define HEADER_HPP

#include <bits/stdint-uintn.h>
#include <iostream>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <cstdint>
#include "channel.hpp"
#include "dragon/global_types.h"
#include "globals.hpp"
#include "magic_numbers.hpp"
#include "obj_queue.hpp"
#include "request.hpp"
#include "utils.hpp"
#include "ugly_macros.hpp"

// NOTE: we can't add any more elements to HeaderType without
// increasing the corresponding number of bits in the header
enum HeaderType
{
    HEADER_TYPE_CTRL_ERR = 0,
    HEADER_TYPE_CTRL_CONN,
    HEADER_TYPE_CTRL_RKEY,
    HEADER_TYPE_CTRL_RTS,
    HEADER_TYPE_CTRL_GETMSG,
    HEADER_TYPE_CTRL_POLL,
    HEADER_TYPE_DATA_EAGER,
    HEADER_TYPE_DATA_RNDV,
    HEADER_TYPE_LAST
};

class Header
{
public:

    // -- BEGIN COMMON-CASE HEADER --
    uint8_t header_and_protocol_type;
    uint8_t timeout;
    eager_size_t eager_size;
    dragonULInt c_uid;
    port_t port;
    // clientid and hints might be 0, in which case
    // thre's no need to send them over the wire
    uint8_t has_clientid_hints;
    uint8_t padding[3];
    // -- END COMMON-CASE HEADER --
    uint64_t clientid;
    uint64_t hints;
    // only used for debugging
    uint64_t seqnum;
    uint64_t checksum;

    // member functions

    void init(HeaderType header_type,
              ProtocolType protocol_type,
              dragonChannelSendReturnWhen_t send_return_mode,
              uint64_t nanoseconds,
              size_t size,
              dragonULInt c_uid,
              port_t port,
              uint64_t clientid,
              uint64_t hints)
    {
        this->header_and_protocol_type = 0u;
        this->eager_size = 0u;
        this->set_type(header_type);
        this->set_protocol_type(protocol_type);
        this->set_send_return_mode(send_return_mode);
        this->set_timeout(nanoseconds);
        this->set_clientid(clientid);
        this->set_hints(hints);

        if (this->has_eager_data())
        {
            assert(size < HSTA_EAGER_SIZE_MAX);
            this->eager_size = size;
        }

        this->c_uid = c_uid;
        this->port  = port;
    }

    void fini() {}

    const char *type_to_string();
    const char *protocol_type_to_string();
    const char *send_return_mode_to_string();

    HeaderType get_type()
    {
        return (HeaderType) ((this->header_and_protocol_type & HEADERTYPE_MASK) >> HEADERTYPE_SHIFT);
    }

    void set_type(HeaderType header_type)
    {
        // TODO: add assertions to verify size constraints
        this->header_and_protocol_type =
            (this->header_and_protocol_type & ~((uint8_t) HEADERTYPE_MASK)) | ((uint8_t) header_type << HEADERTYPE_SHIFT);
    }

    dragonChannelSendReturnWhen_t get_send_return_mode()
    {
        return (dragonChannelSendReturnWhen_t) ((this->header_and_protocol_type & SEND_RETURN_MASK) >> SEND_RETURN_SHIFT);
    }

    void set_send_return_mode(dragonChannelSendReturnWhen_t send_return_mode)
    {
        this->header_and_protocol_type =
            (this->header_and_protocol_type & ~((uint8_t) SEND_RETURN_MASK)) | ((uint8_t) send_return_mode << SEND_RETURN_SHIFT);
    }

    ProtocolType get_protocol_type()
    {
        return (ProtocolType) (this->header_and_protocol_type & PROTOCOLTYPE_MASK);
    }

    void set_protocol_type(ProtocolType protocol_type)
    {
        this->header_and_protocol_type =
            (this->header_and_protocol_type & ~PROTOCOLTYPE_MASK) | (uint8_t) protocol_type;
    }

    uint64_t get_clientid()
    {
        auto has_clientid = (bool) (this->has_clientid_hints & HSTA_CLIENTID_MASK);

        if (has_clientid) {
            return this->clientid;
        } else {
            return HSTA_INVALID_CLIENTID_HINTS;
        }
    }

    uint64_t get_has_clientid()
    {
        return (bool) (this->has_clientid_hints & HSTA_CLIENTID_MASK);
    }

    void set_clientid(uint64_t clientid)
    {
        auto has_clientid = (uint8_t) (clientid != HSTA_INVALID_CLIENTID_HINTS);

        this->has_clientid_hints =
            (this->has_clientid_hints & ~HSTA_CLIENTID_MASK) | has_clientid;

        this->clientid = clientid;
    }

    uint64_t get_hints()
    {
        auto has_hints = (bool) (this->has_clientid_hints & HSTA_HINTS_MASK);

        if (has_hints) {
            return this->hints;
        } else {
            return HSTA_INVALID_CLIENTID_HINTS;
        }
    }

    uint64_t get_has_hints()
    {
        return (bool) (this->has_clientid_hints & HSTA_HINTS_MASK);
    }

    void set_hints(uint64_t hints)
    {
        auto has_hints = (uint8_t) (hints != HSTA_INVALID_CLIENTID_HINTS);

        this->has_clientid_hints =
            (this->has_clientid_hints & ~HSTA_HINTS_MASK) | (has_hints << HSTA_HINTS_SHIFT);

        this->hints = hints;
    }

    // TODO: it's no longer necessary to return size here
    // (we can just get it directly from the header)
    void get_payload_and_size(void **payload, eager_size_t *size)
    {
        // TODO: misaligned ptr access here?
        *size    = (eager_size_t) this->eager_size;
        *payload = (void *) (this + 1);
    }

    void set_timeout(uint64_t nanoseconds)
    {
        if (nanoseconds < 4ul)
        {
            this->timeout = (uint8_t) nanoseconds;
            return;
        }

        // set upper 6 bits based on the shift of the highest order bit

        auto shift = 63ul - __builtin_clzl(nanoseconds);
        this->timeout = shift << TIMEOUT_SHIFT;

        // set lower 2 bits based on the two bits to the right of the highest order bit

        this->timeout |= (nanoseconds >> (shift - TIMEOUT_SHIFT)) & TIMEOUT_MASK;
    }

    uint64_t get_timeout()
    {
        if (this->timeout < 4u)
        {
            return (uint64_t) this->timeout;
        }

        auto shift = this->timeout >> TIMEOUT_SHIFT;
        return ((1ul << TIMEOUT_SHIFT) | (this->timeout & TIMEOUT_MASK)) << (shift - TIMEOUT_SHIFT);
    }

    // TODO: it would make sense to determine this by checking if
    // eager_size == 0
    bool has_eager_data()
    {
        return (   this->get_type() == HEADER_TYPE_CTRL_CONN
                || this->get_type() == HEADER_TYPE_CTRL_RKEY
                || this->get_type() == HEADER_TYPE_CTRL_RTS
                || this->get_type() == HEADER_TYPE_CTRL_POLL
                || this->get_type() == HEADER_TYPE_CTRL_ERR
                || this->get_type() == HEADER_TYPE_DATA_EAGER);
    }

    size_t copy_in(void *buf)
    {
        uint8_t *buf_in = (uint8_t *) buf;

        this->header_and_protocol_type = *buf_in;
        buf_in += sizeof(this->header_and_protocol_type);

        this->timeout = *buf_in;
        buf_in += sizeof(this->timeout);

        this->eager_size = *(eager_size_t *)buf_in;
        buf_in += sizeof(this->eager_size);

        this->c_uid = *(dragonULInt *)buf_in;
        buf_in += sizeof(this->c_uid);

        this->port = *(port_t *)buf_in;
        buf_in += sizeof(this->port);

        this->has_clientid_hints = *(uint8_t *)buf_in;
        buf_in += sizeof(this->has_clientid_hints);

        // need to set has_clientid_hints before using get_has_clientid
        if (this->get_has_clientid()) {
            this->clientid = *(uint64_t *)buf_in;
            buf_in += sizeof(this->clientid);
        }

        // need to set has_clientid_hints before using get_has_hints
        if (this->get_has_hints()) {
            this->hints = *(uint64_t *)buf_in;
            buf_in += sizeof(this->hints);
        }

        if (dragon_hsta_debug) {
            this->seqnum = *(uint64_t *)buf_in;
            buf_in += sizeof(this->seqnum);

            this->checksum = *(uint64_t *)buf_in;
            buf_in += sizeof(this->checksum);
        }

        return (size_t)buf_in - (size_t)buf;
    }

    size_t copy_out(void *buf)
    {
        auto buf_out = (uint8_t *) buf;

        *buf_out = this->header_and_protocol_type;
        buf_out += sizeof(this->header_and_protocol_type);

        *buf_out = this->timeout;
        buf_out += sizeof(this->timeout);

        *(eager_size_t *)buf_out = this->eager_size;
        buf_out += sizeof(this->eager_size);

        *(dragonULInt *)buf_out = this->c_uid;
        buf_out += sizeof(this->c_uid);

        *(port_t *)buf_out = this->port;
        buf_out += sizeof(this->port);

        *(uint8_t *)buf_out = this->has_clientid_hints;
        buf_out += sizeof(this->has_clientid_hints);

        // need to set has_clientid_hints before using get_clientid
        auto clientid = this->get_clientid();
        if (clientid != HSTA_INVALID_CLIENTID_HINTS) {
            *(uint64_t *)buf_out = clientid;
            buf_out += sizeof(this->clientid);
        }

        // need to set has_clientid_hints before using get_hints
        auto hints = this->get_hints();
        if (hints != HSTA_INVALID_CLIENTID_HINTS) {
            *(uint64_t *)buf_out = hints;
            buf_out += sizeof(this->hints);
        }

        if (dragon_hsta_debug) {
            *(uint64_t *)buf_out = this->seqnum;
            buf_out += sizeof(this->seqnum);

            *(uint64_t *)buf_out = this->checksum;
            buf_out += sizeof(this->checksum);
        }

        return (size_t)buf_out - (size_t)buf;
    }
};

DECL_OBJQ_RING_9(
    Header,
    HeaderType,
    ProtocolType,
    dragonChannelSendReturnWhen_t,
    uint64_t,
    size_t,
    dragonULInt,
    port_t,
    uint64_t,
    uint64_t
)

#endif // HEADER_HPP
