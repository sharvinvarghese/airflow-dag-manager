import requests

def get_task_instance(
    dag_name,
    dag_run_id,
    task_id,
    host="localhost:8080"
):
    """
    Fetch details of a specific task instance from the Airflow REST API v2.

    Args:
        dag_name (str): The DAG ID.
        dag_run_id (str): The DAG run ID (e.g. 'manual__2026-05-25T16:17:59.984723+00:00').
        task_id (str): The task ID within the DAG.
        host (str): Airflow host URL (default: localhost:8080).

    Returns:
        dict: JSON response with task instance details including state, operator, map_index.
    """
    url = (
        f"{host}/api/v2/dags/{dag_name}"
        f"/dagRuns/{dag_run_id}"
        f"/taskInstances/{task_id}"
    )

    headers = {
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


# Example usage:
# result = get_task_instance(
#     dag_name="your_dag_id",
#     dag_run_id="manual__2026-05-25T16:17:59.984723+00:00",
#     task_id="your_task_id"
# )
# print(result)
# print(result["state"])
# print(result["operator"])
