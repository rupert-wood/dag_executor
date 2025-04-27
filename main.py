import logging
from typing import Dict, Callable, Any

import time
import random
from dag_executor import DAGExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def task_a() -> str:
    """
    Task A: Simulates some work and returns a result.
    """
    print("Executing Task A")
    time.sleep(random.uniform(1, 3))  # Simulate variable execution time
    result = "Result from A"
    print(f"Task A completed with result: {result}")
    return result

def task_b(a_result: str) -> str:
    """
    Task B: Depends on the result of Task A.
    """
    print("Executing Task B")
    time.sleep(random.uniform(1, 3))
    result = f"Result from B, based on {a_result}"
    print(f"Task B completed with result: {result}")
    return result

def task_c() -> str:
    """
    Task C: Independent task.
    """
    print("Executing Task C")
    time.sleep(random.uniform(1, 3))
    result = "Result from C"
    print(f"Task C completed with result: {result}")
    return result

def task_d(b_result: str, c_result: str) -> str:
    """
    Task D: Depends on results from Task B and Task C.
    """
    print("Executing Task D")
    time.sleep(random.uniform(1, 3))
    result = f"Result from D, based on {b_result} and {c_result}"
    print(f"Task D completed with result: {result}")
    return result



def main() -> None:
    """
    Main function to create and execute the DAG.
    """
    tasks: Dict[str, Callable[..., Any]] = {
        "A": task_a,
        "B": task_b,
        "C": task_c,
        "D": task_d,
    }
    dag: Dict[str, set[str]] = {
        "A": set(),
        "B": {"A"},
        "C": set(),
        "D": {"B", "C"},
    }
    dag_executor = DAGExecutor(tasks, dag)

    results = dag_executor.execute()

    # Print the results.
    print("\n--- Task Results ---")
    for task_name, result in results.items():
        print(f"{task_name}: {result}")

if __name__ == "__main__":
    main()