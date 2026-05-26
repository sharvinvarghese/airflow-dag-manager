from AirFlowDagFetch import get_dag_runs
from AirFlowTaskFetch import get_task_instance
from AirFlowTaskChanges import change_task_state


def main():
    """
    Interactive CLI to inspect DAG runs, view task details, and patch task states
    using the Apache Airflow REST API v2.
    """
    dag_name = input("Enter DAG name: ").strip()

    print("\nFetching DAG runs...\n")
    dag_data = get_dag_runs(dag_name)
    dag_runs = dag_data["dag_runs"]

    for idx, run in enumerate(dag_runs, start=1):
        print(
            f"{idx}. "
            f"{run['dag_run_id']} | "
            f"State={run['state']} | "
            f"RunAfter={run.get('run_after')}"
        )

    selected_run = int(input("\nSelect DAG run number: ")) - 1
    dag_run_id = dag_runs[selected_run]["dag_run_id"]

    task_id = input("\nEnter task_id (example: bb_pipeline): ").strip()

    print("\nFetching task details...\n")
    task_data = get_task_instance(
        dag_name=dag_name,
        dag_run_id=dag_run_id,
        task_id=task_id
    )

    print("\nTask Details")
    print("-------------")
    print("Task ID:", task_data.get("task_id"))
    print("State:", task_data.get("state"))
    print("Operator:", task_data.get("operator"))
    print("Rendered Map Index:", task_data.get("rendered_map_index"))

    rendered_map_index = task_data.get("rendered_map_index")
    map_indexes = [rendered_map_index if rendered_map_index is not None else 0]

    print("\nAvailable Map Indexes\n")
    for idx, val in enumerate(map_indexes, start=1):
        print(f"{idx}. {val}")

    selected_map = int(input("\nSelect map index number: ")) - 1
    chosen_map_index = map_indexes[selected_map]

    print("\nTriggering PATCH state change...\n")
    result = change_task_state(
        dag_name=dag_name,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=chosen_map_index
    )

    print("\nPATCH Success\n")
    print(result)


if __name__ == "__main__":
    main()
