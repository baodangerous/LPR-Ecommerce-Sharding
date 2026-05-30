"""
visualization/plot_results.py
------------------------------
Tất cả biểu đồ cho LPR paper với design palette chuyên nghiệp.

Color palette (thay thế yellow/red/lightgreen):
  Static   → #6B7280  (slate gray   — neutral baseline)
  Reactive → #EF6C5E  (soft coral   — cảnh báo: nhiều overhead)
  LPR      → #3B82C4  (cool blue    — chủ động, tin cậy)

Fonts: sử dụng matplotlib rcParams để đồng nhất toàn bộ figure.
"""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict


# ── Color palette ──────────────────────────────────────────────────────────────
PALETTE = {
    "Static":   "#6B7280",   # slate gray
    "Reactive": "#EF6C5E",   # soft coral/orange-red
    "LPR":      "#3B82C4",   # cool blue
}
EDGE_PALETTE = {
    "Static":   "#4B5563",
    "Reactive": "#C0392B",
    "LPR":      "#1D5B97",
}

# ── rcParams chuẩn ─────────────────────────────────────────────────────────────
RC = {
    "font.family":        "DejaVu Sans",
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.alpha":         0.35,
    "grid.linestyle":     "--",
    "axes.labelsize":     12,
    "xtick.labelsize":    11,
    "ytick.labelsize":    11,
    "legend.fontsize":    11,
    "figure.dpi":         140,
}


def _apply_rc():
    for k, v in RC.items():
        plt.rcParams[k] = v


def _strategy_colors(strategies: List[str]):
    """Trả về list màu theo đúng thứ tự strategies."""
    return [PALETTE.get(s, "#888888") for s in strategies]


def _strategy_edges(strategies: List[str]):
    return [EDGE_PALETTE.get(s, "#555555") for s in strategies]


def _legend_patches(strategies: List[str]):
    return [
        mpatches.Patch(color=PALETTE.get(s, "#888"), label=s)
        for s in strategies
    ]


def plot_throughput(
    strategies: List[str],
    throughputs: List[float],
    title: str = "Throughput comparison",
    save_path: Optional[Path] = None,
) -> plt.Figure:
    _apply_rc()
    fig, ax = plt.subplots(figsize=(7, 4.5))
    colors = _strategy_colors(strategies)
    edges  = _strategy_edges(strategies)
    bars = ax.bar(strategies, throughputs, color=colors, edgecolor=edges, linewidth=0.8, width=0.55)
    ax.set_ylabel("Requests / second")
    ax.set_title(title, fontsize=13, fontweight="500", pad=10)
    # Annotate values on bars
    for bar, val in zip(bars, throughputs):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(throughputs) * 0.015,
            f"{val/1e6:.2f}M",
            ha="center", va="bottom", fontsize=10, color="#374151",
        )
    ax.set_ylim(0, max(throughputs) * 1.18)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
    return fig


def plot_latency(
    strategies: List[str],
    latencies: List[float],
    unit: str = "µs",
    title: str = "Average latency comparison",
    save_path: Optional[Path] = None,
) -> plt.Figure:
    _apply_rc()
    fig, ax = plt.subplots(figsize=(7, 4.5))
    colors = _strategy_colors(strategies)
    edges  = _strategy_edges(strategies)
    bars = ax.bar(strategies, latencies, color=colors, edgecolor=edges, linewidth=0.8, width=0.55)
    ax.set_ylabel(f"Avg latency ({unit})")
    ax.set_title(title, fontsize=13, fontweight="500", pad=10)
    for bar, val in zip(bars, latencies):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(latencies) * 0.015,
            f"{val:.2f}",
            ha="center", va="bottom", fontsize=10, color="#374151",
        )
    ax.set_ylim(0, max(latencies) * 1.2)
    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
    return fig


def plot_migrations(
    strategies: List[str],
    migrations: List[int],
    title: str = "Migration count",
    save_path: Optional[Path] = None,
) -> plt.Figure:
    _apply_rc()
    fig, ax = plt.subplots(figsize=(7, 4.5))
    colors = _strategy_colors(strategies)
    edges  = _strategy_edges(strategies)
    bars = ax.bar(strategies, migrations, color=colors, edgecolor=edges, linewidth=0.8, width=0.55)
    ax.set_ylabel("Total migrations")
    ax.set_title(title, fontsize=13, fontweight="500", pad=10)
    for bar, val in zip(bars, migrations):
        label = str(int(val)) if val > 0 else "0"
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(migrations + [1]) * 0.015,
            label,
            ha="center", va="bottom", fontsize=11, fontweight="500",
            color=EDGE_PALETTE.get(strategies[migrations.index(val)], "#333"),
        )
    ax.set_ylim(0, max(migrations + [1]) * 1.25)
    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
    return fig


def plot_imbalance_over_time(
    imbalances_dict: Dict[str, List[float]],
    title: str = "Imbalance Ratio over time",
    save_path: Optional[Path] = None,
) -> plt.Figure:
    """
    imbalances_dict: {"Static": [...], "Reactive": [...], "LPR": [...]}
    """
    _apply_rc()
    fig, ax = plt.subplots(figsize=(10, 5))
    line_styles = {"Static": "-", "Reactive": "--", "LPR": ":"}
    line_widths  = {"Static": 1.2, "Reactive": 1.4, "LPR": 2.2}

    for name, values in imbalances_dict.items():
        ax.plot(
            range(len(values)), values,
            label=name,
            color=PALETTE.get(name, "#888"),
            linestyle=line_styles.get(name, "-"),
            linewidth=line_widths.get(name, 1.5),
            alpha=0.9,
        )

    ax.set_xlabel("Time step")
    ax.set_ylabel("Imbalance Ratio (IR)")
    ax.set_title(title, fontsize=13, fontweight="500", pad=10)
    ax.legend(handles=_legend_patches(list(imbalances_dict.keys())), loc="upper right")
    ax.axhline(1.0, color="#9CA3AF", linestyle=":", linewidth=0.8, alpha=0.6)
    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
    return fig


def plot_comparison_4panel(
    summary_data: Dict,
    workload_name: str = "",
    save_path: Optional[Path] = None,
) -> plt.Figure:
    """
    summary_data = {
      "strategies": ["Static", "Reactive", "LPR"],
      "throughputs": [...],
      "latencies": [...],
      "migrations": [...],
      "imbalances": [...],
    }
    4-panel figure: Throughput | Latency | Migrations | Imbalance Ratio
    """
    _apply_rc()
    strategies = summary_data["strategies"]
    colors = _strategy_colors(strategies)
    edges  = _strategy_edges(strategies)

    fig, axes = plt.subplots(1, 4, figsize=(18, 5))
    sup = f" — {workload_name} workload" if workload_name else ""
    fig.suptitle(f"LPR vs baselines{sup}", fontsize=14, fontweight="500", y=1.02)

    datasets = [
        ("throughputs", "Throughput (req/s)",     lambda v: f"{v/1e6:.2f}M"),
        ("latencies",   "Avg latency (µs)",        lambda v: f"{v:.2f}"),
        ("migrations",  "Total migrations",         lambda v: str(int(v))),
        ("imbalances",  "Imbalance Ratio",          lambda v: f"{v:.2f}"),
    ]

    for ax, (key, ylabel, fmt) in zip(axes, datasets):
        vals = summary_data[key]
        bars = ax.bar(strategies, vals, color=colors, edgecolor=edges, linewidth=0.8, width=0.6)
        ax.set_ylabel(ylabel, fontsize=11)
        for bar, val in zip(bars, vals):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(vals + [0.001]) * 0.02,
                fmt(val),
                ha="center", va="bottom", fontsize=9.5,
            )
        ax.set_ylim(0, max(vals + [0.001]) * 1.22)
        if key == "throughputs":
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))

    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, bbox_inches="tight")
    return fig
