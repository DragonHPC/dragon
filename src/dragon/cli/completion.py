"""
Shell completion generation for Dragon CLI tools using shtab.

This module provides functions to generate shell completions (bash, zsh)
for the primary Dragon CLI tools.

Usage:
    dragon-install-completions [--shell <shell_name>]
"""

import argparse
import sys
import os

from pathlib import Path
from typing import Optional

try:
    import shtab
except ImportError:
    raise ImportError("shtab is required for shell completion support. Install it with: pip install shtab")


SHELL_BASH = "bash"
SHELL_ZSH = "zsh"
SHELLS = (SHELL_BASH, SHELL_ZSH)

TOOL_DRAGON = "dragon"
TOOL_DRUN = "drun"
TOOL_DHOSTS = "dhosts"
TOOL_DRAGON_CLEANUP = "dragon-cleanup"
TOOL_DRAGON_CONFIG = "dragon-config"
TOOL_DRAGON_NETWORK_CONFIG = "dragon-network-config"


def get_dragon_parser() -> argparse.ArgumentParser:
    """Get the ArgumentParser for the 'dragon' command."""
    from dragon.launcher.launchargs import get_parser

    return get_parser()


def get_drun_parser() -> argparse.ArgumentParser:
    """Get the ArgumentParser for the 'drun' command."""
    from dragon.tools.dragon_run.drun import get_parser

    return get_parser()


def get_dhosts_parser() -> argparse.ArgumentParser:
    """Get the ArgumentParser for the 'dhosts' command."""
    from dragon.tools.dragon_run.dhosts import get_parser

    return get_parser()


def get_dragon_cleanup_parser() -> argparse.ArgumentParser:
    """Get the ArgumentParser for the 'dragon-cleanup' command."""
    from dragon.tools.dragon_cleanup import get_drun_parser

    return get_drun_parser()


def get_dragon_config_parser() -> argparse.ArgumentParser:
    """Get the ArgumentParser for the 'dragon-config' command."""
    from dragon.infrastructure.config import _configure_parser

    return _configure_parser()


def get_dragon_network_config_parser() -> argparse.ArgumentParser:
    """Get the ArgumentParser for the 'dragon-network-config' command."""
    from dragon.launcher.network_config import get_parser

    return get_parser()


TOOL_TO_PARSER_MAP = {
    TOOL_DRAGON: get_dragon_parser,
    TOOL_DRUN: get_drun_parser,
    TOOL_DHOSTS: get_dhosts_parser,
    TOOL_DRAGON_CLEANUP: get_dragon_cleanup_parser,
    TOOL_DRAGON_CONFIG: get_dragon_config_parser,
    TOOL_DRAGON_NETWORK_CONFIG: get_dragon_network_config_parser,
}

TOOLS = TOOL_TO_PARSER_MAP.keys()


def generate_completion(tool_name: str, shell: str = SHELL_BASH) -> str:
    """
    Generate shell completion script for a Dragon CLI tool.

    Args:
        tool_name: One of 'dragon', 'drun', or 'dhosts'
        shell: Target shell - 'bash' or 'zsh' (default: 'bash')
        parent_prog_name: Override the program name in completions (optional)

    Returns:
        Shell completion script as a string

    Raises:
        ValueError: If tool_name or shell is not recognized
    """
    if tool_name not in TOOLS:
        raise ValueError(f"Unknown tool: {tool_name}. Must be one of: {', '.join(TOOLS)}")

    if shell not in SHELLS:
        raise ValueError(f"Unknown shell: {shell}. Must be one of: {', '.join(SHELLS)}")

    # Get the appropriate parser
    parser = TOOL_TO_PARSER_MAP[tool_name]()

    # Generate completion using shtab
    return shtab.complete(parser, shell=shell)


def detect_shell() -> Optional[str]:
    """Detect shell from $SHELL and return one of SUPPORTED_SHELLS."""
    shell_env = os.environ.get("SHELL", "")
    if not shell_env:
        return None

    shell_name = Path(shell_env).name.lower()
    if shell_name in SHELLS:
        return shell_name
    return None


def bash_completion_file() -> Path:
    """Return path where completion script should be installed for bash shells."""
    return Path.home() / ".dragon" / "completions" / f"dragon-completions.bash"


def zsh_completion_file(tool: str) -> Path:
    """Return path where completion script should be installed for zsh shells."""
    return Path.home() / ".dragon" / "completions" / f"_{tool}"


def rc_file_for_shell(shell: str) -> Path:
    """Return shell rc file path used for loading completions."""
    if shell == SHELL_BASH:
        return Path.home() / ".bashrc"
    if shell == SHELL_ZSH:
        return Path.home() / ".zshrc"
    else:
        raise ValueError(f"Unsupported shell: {shell}")


def generate_tab_completions(shell: str) -> bool:
    """Generate completions for all Dragon CLI tools."""

    rc_file_additions = []
    if shell == SHELL_BASH:
        # For bash we can combine all completions into a single file and source it from .bashrc

        completion_file = bash_completion_file()
        completion_file.parent.mkdir(parents=True, exist_ok=True)

        chunks = []
        for tool in TOOLS:
            chunks.append(f"# ---- {tool} completion ----")
            chunks.append(generate_completion(tool, shell=shell).rstrip())
            chunks.append("")

        completion_text = "\n".join(chunks).rstrip() + "\n"
        completion_file.write_text(completion_text, encoding="utf-8")

        # This is the line that will be added to .bashrc to source the completions
        rc_file_additions.append(f"[ -f {completion_file} ] && source {completion_file}")

    elif shell == SHELL_ZSH:
        # zsh needs separate completion files in a directory on the fpath,
        # so we generate one file per tool.

        for tool in TOOLS:
            completion_file = zsh_completion_file(tool)
            completion_file.parent.mkdir(parents=True, exist_ok=True)

            completion_script = generate_completion(tool, shell=shell)

            completion_file.write_text(completion_script, encoding="utf-8")

        # These are the line that will be added to .zshrc to source the completions
        rc_file_additions.append(f"fpath=({completion_file.parent} $fpath)")
        rc_file_additions.append("autoload -U compinit && compinit")
    else:
        raise ValueError(f"Unsupported shell: {shell}")

    return ensure_source_in_rc(shell, "\n".join(rc_file_additions))


def ensure_source_in_rc(shell: str, line: str) -> bool:
    """Ensure rc file sources the completion file. Returns True if updated."""
    rc_file = rc_file_for_shell(shell)

    if rc_file.exists():
        content = rc_file.read_text(encoding="utf-8")
    else:
        content = ""

    if line in content:
        return False

    with rc_file.open("a", encoding="utf-8") as f:
        if content and not content.endswith("\n"):
            f.write("\n")
        f.write("\n# Dragon shell completions\n")
        f.write(f"{line}\n")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dragon-install-completions",
        description=f"Install Dragon shell completions for {', '.join(SHELLS)}.",
    )
    parser.add_argument(
        "shell",
        nargs="?",
        choices=SHELLS,
        help=f"Target shell ({', '.join(SHELLS)}). Auto-detected from $SHELL if omitted.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    shell = args.shell or detect_shell()
    if shell is None:
        print(
            f"Could not determine your shell from $SHELL. Please provide one of: {', '.join(SHELLS)}.",
            file=sys.stderr,
        )
        return 2

    try:
        rc_updated = generate_tab_completions(shell)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    print(f"Installed Dragon completions for {shell}")
    if rc_updated:
        print(f"Updated {rc_file_for_shell(shell)} to source completions.")
    else:
        print(f"{rc_file_for_shell(shell)} already sources completions.")

    print("Open a new shell or source your rc file to activate completions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
