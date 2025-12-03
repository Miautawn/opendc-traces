import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import networkx as nx


def parse_out_xml(path: Path):
    """
    Returns 3 dictionaries:
     1. Parent -> Child task mapping
     2. Child -> Parent task mapping
     2. Each tasks properties (e.g. runtime)
    """
    tree = ET.parse(path)
    root = tree.getroot()

    parent_to_children_map = defaultdict(list)
    children_to_parent_map = defaultdict(list)
    task_properties = {}

    # 1. Get each tasks properties (such as runtime)
    for task in root.findall("{*}job"):
        task_id = task.get("id")
        task_properties[task_id] = {"duration": int(float(task.get("runtime")) * 1000)}

    # 2. Iterate through the Child -> Parent relationships in the XML
    for child_elem in root.findall("{*}child"):
        child_id = child_elem.get("ref")

        for parent_elem in child_elem.findall("{*}parent"):
            parent_id = parent_elem.get("ref")

            # --- THE REVERSAL HAPPENS HERE ---
            # The PARENT ID becomes the KEY.
            # The CHILD ID is appended to the PARENT's list.
            parent_to_children_map[parent_id].append(child_id)
            children_to_parent_map[child_id].append(parent_id)

    return parent_to_children_map, children_to_parent_map, task_properties


def create_graph(parent_to_children_map: dict) -> nx.DiGraph:
    """
    Creates a NetworkX directed graph from a parent_to_children_map
    """

    G = nx.DiGraph()

    # Iterate through the map to add edges: Parent -> Child
    for parent_id, child_ids in parent_to_children_map.items():
        # Add the child node (it might not be explicitly listed in a <job> tag)
        G.add_node(str(parent_id))

        for child_id in child_ids:
            # Add the child node
            G.add_node(str(child_id))

            # Add the edge: Parent (source) is connected to Child (target)
            G.add_edge(str(parent_id), str(child_id))

    return G


def create_toplogical_sort_map(parent_to_children_map: dict) -> dict:
    """
    Creates a topological sort mapping from a parent_to_children_map
    """

    G = create_graph(parent_to_children_map)

    topological_sort = list(nx.topological_sort(G))

    # Create a mapping from task ID to its position in the topological sort
    topological_sort_map = {
        task_id: index for index, task_id in enumerate(topological_sort)
    }

    return topological_sort_map


def rename_parent_to_child_mapping(parent_to_children_map: dict, rename_map: dict):
    """
    Renames the keys and values in the parent_to_children_map
    """

    renamed = {}

    for parent in parent_to_children_map:
        renamed[rename_map[parent]] = [
            rename_map[child] for child in parent_to_children_map[parent]
        ]

    return renamed


def rename_task_properties(task_properties: dict, rename_map: dict):
    """
    Renames task properties dictionary using a rename mapping
    """

    return {rename_map[k]: v for k, v in task_properties.items()}


def get_tasks_from_seed_file(path: Path):
    """
    Returns parent->child; child->parent task mapping and individual task properties from a seed file
    """
    parent_to_children_map, children_to_parent_map, task_properties = parse_out_xml(
        path
    )

    # we wish to rename the tasks according to a topological sort
    # otherwise their naming would be arbitrary (e.g. ID00001, ID00002, etc)
    topological_sort_map = create_toplogical_sort_map(parent_to_children_map)
    parent_to_children_map = rename_parent_to_child_mapping(
        parent_to_children_map, topological_sort_map
    )
    children_to_parent_map = rename_parent_to_child_mapping(
        children_to_parent_map, topological_sort_map
    )
    task_properties = rename_task_properties(task_properties, topological_sort_map)

    return parent_to_children_map, children_to_parent_map, task_properties
