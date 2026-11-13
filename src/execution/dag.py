from __future__ import annotations

from collections import defaultdict

from src.models import PipelineDefinition, TaskDefinition


class DAGError(ValueError):
    pass


class DAG:
    def __init__(self, tasks: list[TaskDefinition]) -> None:
        self.tasks = {task.id: task for task in tasks}
        if len(self.tasks) != len(tasks):
            raise DAGError("Task IDs must be unique.")
        for task in tasks:
            unknown = set(task.depends_on) - set(self.tasks)
            if unknown:
                raise DAGError(f"Task '{task.id}' depends on unknown tasks: {', '.join(sorted(unknown))}")
        self._assert_acyclic()

    def _assert_acyclic(self) -> None:
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(task_id: str) -> None:
            if task_id in visiting:
                raise DAGError("Pipeline graph contains a cycle.")
            if task_id in visited:
                return
            visiting.add(task_id)
            for dependency in self.tasks[task_id].depends_on:
                visit(dependency)
            visiting.remove(task_id)
            visited.add(task_id)

        for task_id in self.tasks:
            visit(task_id)

    def ready(self, complete: set[str], running: set[str]) -> list[TaskDefinition]:
        return [task for task in self.tasks.values() if task.id not in complete | running and set(task.depends_on) <= complete]

    def lineage(self) -> dict[str, list[str]]:
        return {task.id: task.depends_on for task in self.tasks.values()}
