import requests

def change_task_state(
    dag_name,
    dag_run_id,
    task_id,
    map_index,
    host="localhost:8080"
):
    """
    Patch the state of a specific task instance to 'success' via the Airflow REST API v2.

    Args:
        dag_name (str): The DAG ID.
        dag_run_id (str): The DAG run ID.
        task_id (str): The task ID to patch.
        map_index (int): The map index for mapped tasks (use 0 for non-mapped tasks).
        host (str): Airflow host URL (default: localhost:8080).

    Returns:
        dict: JSON response from the PATCH request.
    """
    url = (
        f"{host}/api/v2/dags/{dag_name}"
        f"/dagRuns/{dag_run_id}"
        f"/taskInstances/{task_id}"
    )

    print(f"[TaskChanges] Preparing PATCH for DAG='{dag_name}', run='{dag_run_id}', task='{task_id}', map_index={map_index}")
    print(f"[TaskChanges] PATCH {url}")

    params = {
        "map_index": map_index,
        "update_mask": "success"
    }

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    payload = {
        "new_state": "success",
        "note": "string",
        "include_upstream": False,
        "include_downstream": False,
        "include_future": False,
        "include_past": False
    }

    print(f"[TaskChanges] Payload: new_state=success, map_index={map_index}")

    response = requests.patch(
        url,
        headers=headers,
        params=params,
        json=payload
    )

    print(f"[TaskChanges] Response status: {response.status_code}")

    response.raise_for_status()

    data = response.json()

    print(f"[TaskChanges] PATCH successful — task '{task_id}' state updated to: {data.get('state', 'unknown')}")

    return data
