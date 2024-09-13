import argparse
from datetime import datetime, timedelta
from firebolt.db import connect
from firebolt.client.auth import ClientCredentials
import os
import pandas as pd
import trio
import httpx
from multiprocessing import Process, cpu_count


async def run_session(id, session_id, sqls, headers, engine_url, concurrency):
    async with httpx.AsyncClient(
        headers=headers, timeout=httpx.Timeout(600.0)
    ) as client:
        while True:
            i = session_id - 1
            while i < len(sqls):
                sql_text = sqls[i]
                try:
                    response = await client.post(engine_url, data=sql_text)
                    if response.status_code != 200:
                        print(
                            f"Client {id} session {session_id}: HTTP error {response.status_code}, response: {response.text}"
                        )
                except httpx.RequestError as e:
                    print(f"Client {id} session {session_id}: RequestError {e}")
                    print(
                        f"Client {id} session {session_id}: Error details: {type(e)} - {e}"
                    )
                    if e.request:
                        print(
                            f"Client {id} session {session_id}: Request URL: {e.request.url}"
                        )
                except Exception as e:
                    print(f"Client {id} session {session_id}: Unexpected error {e}")
                i += concurrency


async def run_concurrent_queries(
    query_history,
    id,
    concurrency,
    headers,
    engine_url,
):
    df = pd.read_csv(query_history)
    async with trio.open_nursery() as nursery:
        for session_id in range(concurrency):
            nursery.start_soon(
                run_session,
                id,
                session_id,
                df["query_text"].tolist(),
                headers,
                engine_url,
                concurrency,
            )
        await trio.sleep(300)
        nursery.cancel_scope.cancel()


def check_env_variable(var_name):
    value = os.environ.get(var_name)
    if value is None:
        print(f"Environment variable '{var_name}' is not set.")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Concurrent queries")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=400,
        help="Optional concurrency value per client, defaults to 400",
    )
    parser.add_argument(
        "query_history", nargs="+", help="query_history file names per client"
    )
    args = parser.parse_args()

    env_variables_set = True
    env_variables_set = env_variables_set and check_env_variable("FB_CLIENT_ID")
    env_variables_set = env_variables_set and check_env_variable("FB_CLIENT_SECRET")
    env_variables_set = env_variables_set and check_env_variable("FB_ACCOUNT")
    env_variables_set = env_variables_set and check_env_variable("FB_ENGINE")
    env_variables_set = env_variables_set and check_env_variable("FB_DATABASE")

    if env_variables_set:
        client_id = os.environ["FB_CLIENT_ID"]
        client_secret = os.environ["FB_CLIENT_SECRET"]
        account_name = os.environ["FB_ACCOUNT"]
        engine_name = os.environ["FB_ENGINE"]
        database = os.environ["FB_DATABASE"]
        api_endpoint = os.environ.get("FB_API")
        if api_endpoint is None:
            api_endpoint = "api.app.firebolt.io"

    connection = connect(
        auth=ClientCredentials(client_id, client_secret),
        account_name=account_name,
        engine_name=engine_name,
        database=database,
        api_endpoint=api_endpoint,
    )
    headers = {
        "Authorization": "Bearer " + connection._client._auth.token,
        "Content-Type": "application/json",
    }
    engine_url = (
        connection.engine_url
        + f"?advanced_mode=1&result_cache_max_bytes_item=0&output_format=Null&database={database}&engine={engine_name}"
    )
    cursor = connection.cursor()
    print(f"Warming up {engine_name} engine with checksum(*)...")
    cursor.execute("set warmup='true';")
    cursor.execute("select checksum(*) FROM uservisits;")
    cursor.execute("select checksum(*) FROM rankings;")
    cursor.execute("set warmup='false';")
    print(f"Completed warm up")

    processes = []
    i = 0
    for query_history_file in args.query_history:
        p = Process(
            target=trio.run,
            args=(
                run_concurrent_queries,
                query_history_file,
                i,
                args.concurrency,
                headers,
                engine_url,
            ),
        )
        p.start()
        processes.append(p)
        i += 1
    for p in processes:
        p.join()

    connection.close()


if __name__ == "__main__":
    main()
