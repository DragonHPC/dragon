"""
attach_delay_analysis.py — plot dragon_ext_attach() delay vs. node count.

Reads the per-node-count timing files written by ``ext_attach_publisher.py``
(``write_timing_stats()``), one file per run named ``attach_delay_nnodes_<N>.txt``,
and renders a box-and-whisker plot with node count on the x-axis and attach
latency (ms) on the y-axis.

Each input file contains ``key=value`` lines (nodes, count, min, q1, median,
mean, q3, max) plus a final ``values=`` line holding the raw sorted samples.
When raw values are present they are used directly (true box plot); otherwise
the plot is reconstructed from the five-number summary.

Usage:

    python attach_delay_analysis.py                      # scans CWD
    python attach_delay_analysis.py --input-dir results  # scan another dir
    python attach_delay_analysis.py --output plot.png     # choose output path
"""

import argparse
import glob
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

FILE_GLOB = "attach_delay_nnodes_*.txt"


def parse_stats_file(path):
    """Parse a single attach_delay_nnodes_<N>.txt file into a dict."""
    stats = {}
    values = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, raw = line.split("=", 1)
            if key == "values":
                values = [float(v) for v in raw.split(",") if v]
            elif key in ("nodes", "count"):
                stats[key] = int(float(raw))
            else:
                stats[key] = float(raw)
    stats["values"] = values
    return stats


def collect_stats(input_dir):
    """Load and node-sort all stats files found in input_dir."""
    paths = sorted(glob.glob(os.path.join(input_dir, FILE_GLOB)))
    if not paths:
        raise FileNotFoundError(f"No files matching {FILE_GLOB!r} found in {input_dir!r}")
    entries = [parse_stats_file(p) for p in paths]
    entries.sort(key=lambda s: s["nodes"])
    return entries


def build_plot(entries, output_path):
    node_counts = [s["nodes"] for s in entries]
    positions = list(range(1, len(entries) + 1))

    fig, ax = plt.subplots(figsize=(max(6, 1.2 * len(entries)), 6))

    if all(s["values"] for s in entries):
        # True box plot straight from the raw samples.
        ax.boxplot(
            [s["values"] for s in entries],
            positions=positions,
            showmeans=True,
            whis=(0, 100),  # whiskers span full min..max
        )
    else:
        # Reconstruct from the precomputed five-number summary via bxp().
        bxp_stats = [
            {
                "label": str(s["nodes"]),
                "whislo": s["min"],
                "q1": s["q1"],
                "med": s["median"],
                "q3": s["q3"],
                "whishi": s["max"],
                "mean": s["mean"],
                "fliers": [],
            }
            for s in entries
        ]
        ax.bxp(bxp_stats, positions=positions, showmeans=True)

    ax.set_xticks(positions)
    ax.set_xticklabels([str(n) for n in node_counts])
    ax.set_xlabel("Number of nodes")
    ax.set_ylabel("dragon_ext_attach() latency (ms)")
    ax.set_title("External attach delay vs. node count")
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    print(f"Wrote plot -> {output_path!r}")


def _parse_args():
    parser = argparse.ArgumentParser(description="Plot attach delay vs. node count")
    parser.add_argument(
        "--input-dir",
        default=os.getcwd(),
        help="Directory to scan for attach_delay_nnodes_*.txt files (default: CWD)",
    )
    parser.add_argument(
        "--output",
        default="attach_delay_boxplot.png",
        help="Output image path (default: attach_delay_boxplot.png)",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    entries = collect_stats(args.input_dir)
    build_plot(entries, args.output)


if __name__ == "__main__":
    main()
