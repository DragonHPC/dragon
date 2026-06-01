"""Reporter tool — format results as Markdown table (no HITL)."""


def format_results_table(rows: list, columns: list) -> dict:
    """Render simulation results as a GitHub-flavoured Markdown table.

    :param rows: List of dicts, each dict is one row with column names as keys. Example: [{"N Samples": 1000, "Pi Estimate": 3.14}, ...].
    :param columns: Ordered list of column name strings to include in the table.
    :returns: Dict with ``markdown_table``.
    """
    if not rows or not columns:
        return {"markdown_table": "_No data._"}

    header = "| " + " | ".join(str(c) for c in columns) + " |"
    sep = "| " + " | ".join(":---:" for _ in columns) + " |"
    lines = [header, sep]
    for row in rows:
        lines.append(
            "| " + " | ".join(str(row.get(c, "")) for c in columns) + " |"
        )
    result = {"markdown_table": "\n".join(lines)}
    print(f"[reporter] format_results_table -> {len(lines) - 2} rows rendered", flush=True)
    return result
