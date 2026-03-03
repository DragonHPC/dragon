def main():
    import sys

    from .cli.__main__ import main as _main
    from .infrastructure.facts import PROCNAME_LA_FE

    _main([PROCNAME_LA_FE] + sys.argv[1:])


if __name__ == "__main__":
    main()
