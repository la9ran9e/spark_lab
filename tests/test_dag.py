import pytest

from scheduler.dag import DAG


@pytest.fixture
def dag():
    dag = DAG()
    yield dag


def test_add_edge_not_existent(dag):
    with pytest.raises(KeyError) as exc_info:
        dag.add_edge("foo", "bar")
        assert exc_info.value == f"{'foo'!r} not exists"

    dag.add_node("foo")
    with pytest.raises(KeyError) as exc_info:
        dag.add_edge("foo", "bar")
        assert exc_info.value == f"{'bar'!r} not exists"
