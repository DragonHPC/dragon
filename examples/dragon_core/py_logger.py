import logging

from dragon.dlogging.util import setup_BE_logging


def main():
    _level, _fname = setup_BE_logging(service="My Application")
    log = logging.getLogger("user_app").getChild("multinode")

    log.warning("Hello from Python!")


if __name__ == "__main__":
    main()