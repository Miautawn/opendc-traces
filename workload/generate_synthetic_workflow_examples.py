"""Multiple experimental workflow generators and a small runner.

Each generator returns (df_tasks, df_fragments, name). The runner writes traces
using existing `write_workload_trace.writeTrace` and visualizes the DAG using
`visualize_workflow.visualize_dag`.

Examples included:
- mostly_parallel: many independent tasks feeding a small aggregator
- highly_dependent: a long chain of dependent tasks
- long_workload: tasks with long durations
- balanced_tree: a tree-shaped DAG
- random_dag: random DAG for stress testing
"""

import random
import time
from pathlib import Path
from typing import List, Tuple

import networkx as nx
import pandas as pd
import scipy
from schemas.workload_schema_v5 import workload_schema
from tqdm import tqdm
from utils.visualize_workflow import visualize_dag
from utils.write_workload_trace import writeTrace


def generate_workflow(kind: str, params: dict):
    """Dispatch generator for the given kind and params.

    NOTE: this doesn't generate children columns; only parents.
    The children values are added retroactively when writing the trace.

    Returns (df_tasks, df_fragments, name)
    """
    start_time = pd.to_datetime("2024-01-01", utc=True)
    start_time_ms = int(start_time.timestamp() * 1000)

    if kind == "mostly_parallel":
        n = int(params.get("n", 12))
        tasks = []
        fragments = []
        for i in range(n):
            tasks.append([i, start_time_ms, 60_000, 1, 1_000, 500, [], []])
            fragments.append([i, 60_000, 0.5])
        agg_id = n
        parents = list(range(n))
        tasks.append([agg_id, start_time_ms, 30_000, 1, 500, 200, parents, []])
        fragments.append([agg_id, 30_000, 0.2])
        df_tasks = pd.DataFrame(
            tasks,
            columns=[
                "id",
                "submission_time",
                "duration",
                "cpu_count",
                "cpu_capacity",
                "mem_capacity",
                "parents",
                "children",
            ],
        )
        df_fragments = pd.DataFrame(fragments, columns=["id", "duration", "cpu_usage"])
        df_tasks["submission_time"] = start_time_ms
        return df_tasks, df_fragments, "mostly_parallel"

    if kind == "highly_dependent":
        length = int(params.get("length", 10))
        tasks = []
        fragments = []
        parents = []
        for i in range(length):
            tasks.append([i, start_time_ms, 120_000, 1, 1000, 500, parents.copy(), []])
            fragments.append([i, 120_000, 1.0])
            parents = [i]
        df_tasks = pd.DataFrame(
            tasks,
            columns=[
                "id",
                "submission_time",
                "duration",
                "cpu_count",
                "cpu_capacity",
                "mem_capacity",
                "parents",
                "children",
            ],
        )
        df_fragments = pd.DataFrame(fragments, columns=["id", "duration", "cpu_usage"])
        df_tasks["submission_time"] = start_time_ms
        return df_tasks, df_fragments, "highly_dependent"

    if kind == "long_workload":
        n = int(params.get("n", 6))
        duration_ms = int(params.get("duration_ms", 3_600_000))
        tasks = []
        fragments = []
        for i in range(n):
            tasks.append([i, start_time_ms, duration_ms, 2, 2_000, 8_000, [], []])
            fragments.append([i, duration_ms, 1.5])
        df_tasks = pd.DataFrame(
            tasks,
            columns=[
                "id",
                "submission_time",
                "duration",
                "cpu_count",
                "cpu_capacity",
                "mem_capacity",
                "parents",
                "children",
            ],
        )
        df_fragments = pd.DataFrame(fragments, columns=["id", "duration", "cpu_usage"])
        df_tasks["submission_time"] = start_time_ms
        return df_tasks, df_fragments, "long_workload"

    if kind == "balanced_tree":
        depth = int(params.get("depth", 3))
        branching = int(params.get("branching", 2))
        tasks = []
        fragments = []
        next_id = 0
        level_nodes = []
        root = next_id
        tasks.append([root, start_time_ms, 30_000, 1, 1000, 500, [], []])
        fragments.append([root, 30_000, 0.5])
        level_nodes = [root]
        next_id += 1
        for d in range(depth):
            new_level = []
            for parent in level_nodes:
                for b in range(branching):
                    nid = next_id
                    tasks.append(
                        [nid, start_time_ms, 30_000, 1, 500, 200, [parent], []]
                    )
                    fragments.append([nid, 30_000, 0.3])
                    new_level.append(nid)
                    next_id += 1
            level_nodes = new_level
        df_tasks = pd.DataFrame(
            tasks,
            columns=[
                "id",
                "submission_time",
                "duration",
                "cpu_count",
                "cpu_capacity",
                "mem_capacity",
                "parents",
                "children",
            ],
        )
        df_fragments = pd.DataFrame(fragments, columns=["id", "duration", "cpu_usage"])
        df_tasks["submission_time"] = start_time_ms
        return df_tasks, df_fragments, "balanced_tree"

    if kind == "random_dag":
        n = int(params.get("n", 15))
        edge_prob = float(params.get("edge_prob", 0.12))
        seed = params.get("seed", None)
        random.seed(seed)
        tasks = []
        fragments = []
        parents_map = {i: [] for i in range(n)}
        for i in range(n):
            for j in range(i + 1, n):
                if random.random() < edge_prob:
                    parents_map[j].append(i)
        for i in range(n):
            p = parents_map[i]
            tasks.append(
                [
                    i,
                    start_time_ms,
                    45_000 + random.randint(0, 60_000),
                    1,
                    1000 + random.random() * 500,
                    500 + random.randint(0, 2000),
                    p,
                    [],
                ]
            )
            fragments.append([i, 45_000, 0.5 + random.random()])
        df_tasks = pd.DataFrame(
            tasks,
            columns=[
                "id",
                "submission_time",
                "duration",
                "cpu_count",
                "cpu_capacity",
                "mem_capacity",
                "parents",
                "children",
            ],
        )
        df_fragments = pd.DataFrame(fragments, columns=["id", "duration", "cpu_usage"])
        df_tasks["submission_time"] = start_time_ms
        return df_tasks, df_fragments, "random_dag"

    raise ValueError(f"Unknown workflow kind: {kind}")


def _write_three_variants(
    df_t,
    df_f,
    base_name: str,
    write_dir: Path,
    write: bool = True,
    visualize: bool = True,
):
    """Write three variants for a generated workflow:
    - original traces under traces/{base_name}/
    - default deadline under traces/{base_name}_deadline_default/ (deadline = submission + duration//2)
    - alt deadline under traces/{base_name}_deadline_alt/ (deadline = submission + duration)
    """

    def normalize_lists(df):
        df = df.copy()
        df["parents"] = df["parents"].apply(lambda x: list(x) if x is not None else [])
        df["children"] = df["children"].apply(
            lambda x: list(x) if x is not None else []
        )
        return df

    df_orig = normalize_lists(df_t)

    # original
    folder_orig = write_dir / base_name
    if write:
        try:
            writeTrace(df_orig, df_f, workload_schema, folder_orig)
        except Exception:
            try:
                writeTrace(df_orig, df_f, folder_orig)
            except Exception:
                print(f"Could not write original traces for {base_name}")
    if visualize:
        try:
            visualize_dag(
                df_orig,
                title=f"{base_name} (original)",
                save_path=f"{folder_orig}/dag.png",
                show=False,
            )
        except Exception as e:
            print(f"Visualization failed for {base_name} original: {e}")

    # default deadline variant: uniform deadline based on critical path + padding
    def compute_uniform_deadline_for_df(df, padding_ms: int = 60_000_000):
        df2 = normalize_lists(df)
        start_time = int(df2["submission_time"].min())
        # build graph and compute critical path
        G = nx.DiGraph()
        for _, row in df2.iterrows():
            nid = int(row["id"])
            G.add_node(nid)
        for _, row in df2.iterrows():
            child = int(row["id"])
            for p in row["parents"]:
                G.add_edge(int(p), child)
        if not nx.is_directed_acyclic_graph(G):
            crit = int(df2["duration"].sum())
        else:
            durations = {int(r["id"]): int(r["duration"]) for _, r in df2.iterrows()}
            longest = {n: 0 for n in G.nodes()}
            for n in nx.topological_sort(G):
                preds = list(G.predecessors(n))
                if preds:
                    longest[n] = durations[n] + max(longest[p] for p in preds)
                else:
                    longest[n] = durations[n]
            crit = max(longest.values()) if longest else 0
        return start_time + int(crit) + int(padding_ms)

    df_def = df_orig.copy()
    uniform_deadline = compute_uniform_deadline_for_df(df_def)
    df_def["deferrable"] = True
    df_def["deadline"] = uniform_deadline
    folder_def = write_dir / f"{base_name}_deadline_default"
    if write:
        try:
            writeTrace(df_def, df_f, workload_schema, folder_def)
        except Exception:
            try:
                writeTrace(df_def, df_f, folder_def)
            except Exception:
                print(f"Could not write default-deadline traces for {base_name}")
    if visualize:
        try:
            visualize_dag(
                df_def,
                title=f"{base_name} (deadline default)",
                save_path=f"{folder_def}/dag.png",
                show=False,
            )
        except Exception as e:
            print(f"Visualization failed for {base_name} default-deadline: {e}")

    # alternative deadline variant (relaxed) - use same uniform deadline but mark deferrable
    df_alt = df_orig.copy()
    df_alt["deferrable"] = True
    df_alt["deadline"] = uniform_deadline
    folder_alt = write_dir / f"{base_name}_deadline_alt"
    if write:
        try:
            writeTrace(df_alt, df_f, workload_schema, folder_alt)
        except Exception:
            try:
                writeTrace(df_alt, df_f, folder_alt)
            except Exception:
                print(f"Could not write alt-deadline traces for {base_name}")
    if visualize:
        try:
            visualize_dag(
                df_alt,
                title=f"{base_name} (deadline alt)",
                save_path=f"{folder_alt}/dag.png",
                show=False,
            )
        except Exception as e:
            print(f"Visualization failed for {base_name} alt-deadline: {e}")


def fill_children(df: pd.DataFrame) -> pd.DataFrame:
    """Given a dataframe of tasks with valid parent ids
    this function inverts this relationship and fills the children column

    Args:
        df (pd.DataFrame): input tasks dataframe with valid parents

    Returns:
        pd.DataFrame: input tasks dataframe with valid children ids
    """

    df_ = df.copy()
    df_["children"] = None

    relationships = df_[["id", "parents"]].explode("parents")
    children_map = relationships.groupby("parents")["id"].apply(list).to_dict()

    df_["children"] = df_["id"].map(children_map)
    df_["children"] = (
        df_["children"].fillna("").apply(list)
    )  # fill with empty lists if no children

    return df_


if __name__ == "__main__":
    # quick demo: run and save visualizations for all variants

    def make_suffix(params: dict):
        return "".join([f"_{k}{v}" for k, v in params.items()])

    def run_sweep(write_dir: Path, write: bool = True, visualize: bool = True):
        # parameter grids for each generator (use string keys)
        sweep_plan = []

        # mostly_parallel: vary number of independent tasks
        for n in [6, 12, 30, 100]:
            sweep_plan.append(("mostly_parallel", {"n": n}))

        # highly_dependent: vary chain length
        for length in [5, 10, 20, 500]:
            sweep_plan.append(("highly_dependent", {"length": length}))

        # long_workload: vary number and duration
        for n in [3, 6, 12]:
            for dur in [60_000, 600_000, 3_600_000, 21_600_000]:
                sweep_plan.append(("long_workload", {"n": n, "duration_ms": dur}))

        # balanced_tree: vary depth and branching
        for depth in [2, 3, 4]:
            for branching in [2, 3, 5]:
                sweep_plan.append(
                    ("balanced_tree", {"depth": depth, "branching": branching})
                )

        # random_dag: vary size and connectivity
        for n in [10, 15, 25]:
            for p in [0.05, 0.12, 0.25, 0.5]:
                for seed in [0, 1]:
                    sweep_plan.append(
                        ("random_dag", {"n": n, "edge_prob": p, "seed": seed})
                    )

        # run the plan
        for func, params in tqdm(sweep_plan, desc="Generating workflows..."):
            df_t, df_f, name = generate_workflow(func, params)
            df_t = fill_children(df_t)

            # retro-actively add children columns based on parents

            # create a unique output name including params
            suffix = make_suffix(
                {
                    k: params[k]
                    for k in params
                    if isinstance(params[k], (int, float, str))
                }
            )
            full_name = f"{name}{suffix}"
            # write original + two deadline variants for each sweep item
            _write_three_variants(
                df_t,
                df_f,
                full_name,
                write_dir=write_dir,
                write=write,
                visualize=visualize,
            )

    run_sweep(write=True, visualize=True, write_dir=Path("synthetic_workflow_traces"))
