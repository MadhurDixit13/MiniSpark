import networkx as nx
import matplotlib.pyplot as plt

def visualize(rdd):
    '''
    Traverse RDD lineage and draw a DAG.
    Falls back gracefully if both pygraphviz and pydot are unavailable.
    '''
    G = nx.DiGraph()
    labels = {}

    def add_node(node):
        nid = id(node)
        label = getattr(node, 'ttype', 'root')
        labels[nid] = label
        G.add_node(nid)
        if getattr(node, 'prev', None):
            prev_id = id(node.prev)
            add_node(node.prev)
            G.add_edge(prev_id, nid)

    add_node(rdd)

    # Attempt Graphviz layout via pygraphviz
    try:
        from networkx.drawing.nx_agraph import graphviz_layout
        pos = graphviz_layout(G, prog='dot')
    except (ImportError, ModuleNotFoundError):
        # Fallback to pydot if available
        try:
            from networkx.drawing.nx_pydot import graphviz_layout as pd_layout
            pos = pd_layout(G, prog='dot')
        except (ImportError, ModuleNotFoundError):
            # Final fallback to spring layout
            pos = nx.spring_layout(G)

    nx.draw(G, pos, labels=labels, with_labels=True, arrows=True, node_size=1500)
    plt.show()