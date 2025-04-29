import capnp
import dragon.infrastructure.message_defs_capnp as schema


def main():
    f = open("test.out", "rb")
    msg = schema.MessageDef.read_packed(f)
    print(msg.which())
    print(msg.to_dict())


if __name__ == "__main__":
    main()
