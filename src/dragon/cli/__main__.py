def main(args=None):
    import argparse
    import sys

    from . import console_scripts

    name_padding = min(24, max(map(len, console_scripts.keys())))

    parser = argparse.ArgumentParser(
        prog="dragon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage="SCRIPT ...",
        description="Run dragon console script",
        epilog="console scripts:\n"
        + "\n".join(f"  {{:<{name_padding}}}  {{:<}}".format(ep.name, ep.value) for ep in console_scripts.values()),
        add_help=False,
    )

    parser.add_argument(
        "script",
        nargs="?",
        choices=console_scripts.keys(),
        help=argparse.SUPPRESS,
    )

    args, remaining = parser.parse_known_args(args)

    # Print help by default
    if not args.script:
        parser.print_help()
        parser.exit(1)

    # Patch sys.argv so the console script believes it was called correctly
    sys.argv.clear()
    sys.argv.append(args.script)
    sys.argv.extend(remaining)

    # Load and exec the console script
    _main = console_scripts[args.script].load()
    _main()


if __name__ == "__main__":
    main()
