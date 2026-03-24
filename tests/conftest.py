"""Pytest conftest - mock Airflow modules for local testing."""

import sys
from types import ModuleType
from unittest.mock import MagicMock


def _install_airflow_mock():
    """Install a fake airflow package so DAG files can be imported locally."""
    if "airflow" in sys.modules:
        return

    # Create mock modules
    airflow = ModuleType("airflow")
    airflow_models = ModuleType("airflow.models")
    airflow_operators = ModuleType("airflow.operators")
    airflow_operators_python = ModuleType("airflow.operators.python")

    # --- DAG mock ---
    class FakeDAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
            self.schedule_interval = kwargs.get("schedule")
            self.catchup = kwargs.get("catchup", True)
            self.tags = kwargs.get("tags", [])
            self.default_args = kwargs.get("default_args", {})
            self._tasks = {}

        def __enter__(self):
            FakeDAG._current = self
            return self

        def __exit__(self, *args):
            FakeDAG._current = None

        @property
        def tasks(self):
            return list(self._tasks.values())

        def get_task(self, task_id):
            return self._tasks.get(task_id)

    # --- PythonOperator mock ---
    class FakePythonOperator:
        def __init__(self, task_id, python_callable, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.downstream_list = []
            self.upstream_list = []
            # Register with current DAG
            if hasattr(FakeDAG, "_current") and FakeDAG._current:
                FakeDAG._current._tasks[task_id] = self

        def __rshift__(self, other):
            """task1 >> task2"""
            if isinstance(other, list):
                for o in other:
                    self.downstream_list.append(o)
                    o.upstream_list.append(self)
            else:
                self.downstream_list.append(other)
                other.upstream_list.append(self)
            return other

        def __rrshift__(self, other):
            """[task1, task2] >> task3"""
            if isinstance(other, list):
                for o in other:
                    o.downstream_list.append(self)
                    self.upstream_list.append(o)
            return self

    # Wire up modules
    airflow.DAG = FakeDAG
    airflow_models.DAG = FakeDAG
    airflow_operators_python.PythonOperator = FakePythonOperator

    sys.modules["airflow"] = airflow
    sys.modules["airflow.models"] = airflow_models
    sys.modules["airflow.operators"] = airflow_operators
    sys.modules["airflow.operators.python"] = airflow_operators_python


# Install before any test imports
_install_airflow_mock()
