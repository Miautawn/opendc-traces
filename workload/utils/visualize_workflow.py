"""Utilities to build and visualize workflow DAGs from task DataFrames.

This module uses NetworkX + matplotlib to draw DAGs. It attempts to use graphviz layout
if available (via pygraphviz/pygraphviz bindings) otherwise falls back to a spring layout.
"""
from typing import Optional
import os
import networkx as nx
import matplotlib.pyplot as plt


def build_graph_from_tasks(df_tasks):
    """Return a NetworkX DiGraph built from a tasks DataFrame.

    Expects a column `id` and optionally `parents` (iterable of parent ids) for each row.
    Parents can be empty list, None or missing.
    Node attributes saved: duration, cpu_count, cpu_capacity, mem_capacity.
    """
    G = nx.DiGraph()
    for _, row in df_tasks.iterrows():
        nid = str(row["id"]) if not pd_is_scalar(row["id"]) else str(int(row["id"]))
        # add node with selected attributes
        G.add_node(nid, duration=row.get("duration"), cpu_count=row.get("cpu_count"), cpu_capacity=row.get("cpu_capacity"), mem_capacity=row.get("mem_capacity"))

        parents = row.get("parents") if "parents" in row.index else None
        if parents is None:
            continue
        # allow both a list and single scalar parent
        try:
            iterable = list(parents)
        except Exception:
            iterable = [parents]
        for p in iterable:
            if p is None:
                continue
            G.add_edge(str(p), nid)

    return G


def pd_is_scalar(v):
    # pandas may give numpy types; treat ints/floats/strings as scalar
    try:
        import pandas as pd
        return pd.api.types.is_scalar(v)
    except Exception:
        return not hasattr(v, "__iter__")


def visualize_dag(df_tasks, title: Optional[str] = None, figsize=(8, 6), save_path: Optional[str] = None, show: bool = True):
    """Draw a DAG from `df_tasks`.

    Parameters
    - df_tasks: pandas DataFrame with at least `id` and optional `parents` column.
    - title: plot title
    - figsize: matplotlib figure size
    - save_path: if provided, save PNG to this path (directories created as needed)
    - show: whether to call plt.show()
    """
    import pandas as pd

    G = build_graph_from_tasks(df_tasks)

    plt.figure(figsize=figsize)
    # try Graphviz (dot) layout first for hierarchical view
    # fall back to pure-networkx layouts that avoid optional heavy deps like scipy
    pos = None
    try:
        pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
    except Exception:
        try:
            pos = nx.nx_pydot.graphviz_layout(G, prog="dot")
        except Exception:
            # avoid calling layouts that may import optional packages (scipy)
            # prefer kamada_kawai which only needs numpy; if that fails, use circular
            try:
                pos = nx.kamada_kawai_layout(G)
            except Exception:
                pos = nx.circular_layout(G)

    # node sizes based on duration (if available)
    durations = []
    for n in G.nodes():
        d = G.nodes[n].get("duration")
        try:
            durations.append(max(100, float(d) / 1000.0))
        except Exception:
            durations.append(300)

    nx.draw(G, pos, with_labels=True, node_size=durations, arrowsize=20)

    if title:
        plt.title(title)

    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, bbox_inches="tight")

    if show:
        plt.show()

    plt.close()
