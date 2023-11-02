from graphql import FieldNode


def get_child_field_names(node: FieldNode) -> list[str]:
    selection_set = node.selection_set
    if selection_set is None:
        return []

    child_node_names = []
    for selection in selection_set.selections:
        if not isinstance(selection, FieldNode):
            # TODO: Use visitor to transform query on the fly
            continue

        child_node_names.append(selection.name.value)

    return child_node_names
