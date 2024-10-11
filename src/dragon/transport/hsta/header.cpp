#include "agent.hpp"
#include "dragon/global_types.h"
#include "header.hpp"
#include "request.hpp"
#include <cstdint>

// class definitions

const char *Header::type_to_string()
{
    switch (this->get_type())
    {
        case HEADER_TYPE_CTRL_ERR:
        {
            return "CTRL_ERR";
        }
        case HEADER_TYPE_CTRL_CONN:
        {
            return "CTRL_CONN";
        }
        case HEADER_TYPE_CTRL_RKEY:
        {
            return "CTRL_RKEY";
        }
        case HEADER_TYPE_CTRL_RTS:
        {
            return "CTRL_RTS";
        }
        case HEADER_TYPE_CTRL_GETMSG:
        {
            return "CTRL_GETMSG";
        }
        case HEADER_TYPE_CTRL_POLL:
        {
            return "CTRL_POLL";
        }
        case HEADER_TYPE_DATA_EAGER:
        {
            return "DATA_EAGER";
        }
        case HEADER_TYPE_DATA_RNDV:
        {
            return "DATA_RNDV";
        }
        case HEADER_TYPE_LAST:
        {
            return "LAST";
        }
        default:
        {
            hsta_default_case(this->get_type());
        }
    }

    return nullptr;
}

const char *Header::protocol_type_to_string()
{
    switch (this->get_protocol_type())
    {
        case PROTOCOL_TYPE_SENDMSG:
        {
            return "SENDMSG";
        }
        case PROTOCOL_TYPE_GETMSG:
        {
            return "GETMSG";
        }
        case PROTOCOL_TYPE_POLL:
        {
            return "POLL";
        }
        case PROTOCOL_TYPE_AGGREGATED:
        {
            return "AGGREGATED";
        }
        case PROTOCOL_TYPE_LAST:
        {
            return "LAST";
        }
        default:
        {
            hsta_default_case(this->get_protocol_type());
        }
    }

    return nullptr;
}

const char *Header::send_return_mode_to_string()
{
    switch (this->get_send_return_mode())
    {
        case DRAGON_CHANNEL_SEND_RETURN_IMMEDIATELY:
        {
            return "IMMEDIATELY";
        }
        case DRAGON_CHANNEL_SEND_RETURN_WHEN_BUFFERED:
        {
            return "WHEN_BUFFERED";
        }
        case DRAGON_CHANNEL_SEND_RETURN_WHEN_RECEIVED:
        {
            return "WHEN_RECEIVED";
        }
        case DRAGON_CHANNEL_SEND_RETURN_WHEN_DEPOSITED:
        {
            return "WHEN_DEPOSITED";
        }
        case DRAGON_CHANNEL_SEND_RETURN_WHEN_NONE:
        {
            return "WHEN_NONE";
        }
        default:
        {
            hsta_default_case(this->get_send_return_mode());
        }
    }

    return nullptr;
}

