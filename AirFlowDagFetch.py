import requests

def get_dag_runs(
    dag_name,
    host="localhost:8080",
    limit=50,
    offset=0,
    order_by="id"
):
    """
    Fetch all DAG runs for a given DAG name from the Airflow REST API v2.

    Args:
        dag_name (str): The DAG ID to query.
        host (str): Airflow host URL (default: localhost:8080).
        limit (int): Max number of runs to return (default: 50).
        offset (int): Pagination offset (default: 0).
        order_by (str): Field to sort results by (default: 'id').

    Returns:
        dict: JSON response containing 'dag_runs' list, sorted by run_after descending.
    """
    url = f"{host}/api/v2/dags/{dag_name}/dagRuns"

    params = {
        "limit": limit,
        "offset": offset,
        "order_by": order_by
    }

    headers = {
        "accept": "application/json"
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        auth=("admin", "admin")  # CHANGE THIS — use env vars or a config file in production
    )

    print(response.status_code)
    print(response.text)

    response.raise_for_status()

    data = response.json()

    data["dag_runs"] = sorted(
        data["dag_runs"],
        key=lambda x: x.get("run_after", ""),
        reverse=True
    )

    return data


# if __name__ == "__main__":
#     result = get_dag_runs("your_dag_id")
#     print(result["dag_runs"][0])
