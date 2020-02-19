class CyclicDependenceError(Exception):
    ...


class DAG:
    def __init__(self):
        self.graph = dict()

    def add_node(self, node):
        if node in self.graph:
            raise KeyError(f"{node!r} already exists")
        self.graph[node] = set()

    def add_edge(self, ind_node, dep_node):
        if ind_node not in self.graph:
            raise KeyError(f"{ind_node!r} not exists")
        if dep_node not in self.graph:
            raise KeyError(f"{dep_node!r} not exists")
        if ind_node in self.downstream(dep_node):
            raise CyclicDependenceError()
        self.graph[ind_node].add(dep_node)

    def travers(self):
        independent = self.get_independent()
        yield from self._do_travers(independent)

    def _do_travers(self, nodes):
        for node in nodes:
            dep_nodes = self.graph[node]
            yield from self._do_travers(dep_nodes)
            yield node

    def get_independent(self):
        dependent = self.get_dependent()
        return set(node for node in self.graph.keys() if node not in dependent)

    def get_dependent(self):
        return set(node for dependent in self.graph.values() for node in dependent)

    def downstream(self, node):
        nodes = self.graph[node]
        return set(node for node in self._do_travers(nodes))


# dag = DAG()
# dag.add_node(1)
# dag.add_node(2)
# dag.add_edge(2, 1)
# dag.add_node(3)
# dag.add_edge(3, 2)
# dag.add_node(4)
# dag.add_edge(1, 4)
# dag.add_edge(2, 4)
# dag.add_node(5)
#
# print(dag.graph)
# for i in dag.travers():
#     print("running now:", i)

#
# '''
#     -r00 -       - r01
#     /   |  \     /   |
#    /    |   \   /    |
#   v     v    v v     v
#  r10   r11   r12    r13
#   -      |   |      -
#    \     |   |     /
#     \    |   |    /
#      \   |   |   /
#       \  v   v  /
#        v  r20  v
# '''
#
# dag = DAG()
# dag.add_node("r00")
# dag.add_node("r01")
# dag.add_node("r10")
# dag.add_node("r11")
# dag.add_node("r12")
# dag.add_node("r13")
# dag.add_node("r20")
#
#
# dag.add_edge("r20", "r10")
# dag.add_edge("r20", "r11")
# dag.add_edge("r20", "r12")
# dag.add_edge("r20", "r13")
# dag.add_edge("r10", "r00")
# dag.add_edge("r11", "r00")
# dag.add_edge("r12", "r00")
# dag.add_edge("r12", "r01")
# dag.add_edge("r13", "r01")
#
#
# print(dag.graph)
# for i in dag.travers():
#     print("running now:", i)
