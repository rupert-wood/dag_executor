import concurrent.futures
import graphlib
import logging
from typing import Dict, Callable, Any

class DAGExecutor:
    """
    A simple class for executing Directed Acyclic Graphs (DAGs) of tasks using a
    ThreadPoolExecutor.
    """

    def __init__(self, tasks: Dict[str, Callable[..., Any]], dag: Dict[str, set[str]]):
        """
        Initializes the DAGExecutor with the task DAG and dependencies.

        Args:
            tasks: A dictionary representing the DAG of tasks.
            dag: A dictionary representing the dependencies between tasks.
        """
        self.tasks = tasks
        self.dag = dag
        self.sorter = graphlib.TopologicalSorter(self.dag)
        self.sorter.prepare()
        self.results: Dict[str, Any] = {}

    def execute(self) -> Dict[str, Any]:
        """
        Executes the DAG using a ThreadPoolExecutor.

        Returns:
            A dictionary containing the results of each task, where keys are task names
            and values are the corresponding results.
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures: Dict[str, concurrent.futures.Future] = {}
            while self.sorter.is_active():
                for task_name in self.sorter.get_ready():
                    task_func = self.tasks[task_name]
                    logging.info(f"Submitting task: {task_name}")
                    prior_results = [self.results[dependency] for dependency in self.dag[task_name]]
                    futures[task_name] = executor.submit(task_func, *prior_results)  # Store the future

                # Collect results as they become available.
                for task, f in futures.items():
                    if f.done():
                        self.results[task] = f.result()
                        self.sorter.done(task)
                        del futures[task]
                        break  # Process one completed task per iteration

        return self.results




