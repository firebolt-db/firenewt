import argparse
import base64
from collections import OrderedDict
import pandas as pd
import json
import firebolt.db
from firebolt.client.auth import ClientCredentials
import numpy as np
import os
import random
import string
import time

import httpx


def format_query_templates(query_history_df, region):
    for index, row in query_history_df.iterrows():
        query_text = row["query_text"]
        if not region:
            if "{{region}}" in query_text:
                raise ValueError("This query history requires environment variable FB_REGION to be set.")
        else:
            query_text = query_text.replace("{{region}}", region)
        query_history_df.at[index, "query_text"] = query_text


def run_powerrun(query_history, client_id, client_secret, account_name, engine_name, database, api_endpoint, region,
                 raw_http_query=False):
    df = pd.read_csv(query_history)
    df.sort_values("query_start_ts", inplace=True)
    format_query_templates(df, region)
    connection = firebolt.db.connect(
        auth=ClientCredentials(
            client_id=client_id,
            client_secret=client_secret,
        ),
        account_name=account_name,
        engine_name=engine_name,
        database=database,
        api_endpoint=api_endpoint,
    )
    headers = {
                "Authorization": "Bearer " + connection._client._auth.token,
                "Content-Type": "application/json",
            }    
    cursor = connection.cursor()
    start_time = None
    characters = string.digits
    random_number = "".join(random.choice(characters) for _ in range(6))
    run_id = f'powerrun_{random_number}_{time.strftime("date_%Y_%m_%d_time_%H_%M_%S")}'
    print(f"Run id is {run_id}\n")

    request_times = OrderedDict()
    with httpx.Client(timeout=httpx.Timeout(7200.0)) as client:
        for index, row in df.iterrows():                
            query_label_decoded = json.dumps({"rid": run_id, "sql_id": row["query_id"]})
            query_label = f'b64-{base64.b64encode(query_label_decoded.encode()).decode()}'

            if raw_http_query:
                engine_url = connection.engine_url + f"?advanced_mode=1&result_cache_max_bytes_item=0&database={database}&engine={engine_name}&query_label={query_label}"
                t1 = time.time()
                response = client.post(engine_url, data=row["query_text"], headers=headers)
                t2 = time.time()
            else:
                cursor.execute(f'set query_label={query_label}')
                t1 = time.time()
                # skip_parsing significantly speeds up the client side part of this query. This can greatly affect
                # certain cases, such as INSERT queries with 1000 literal row VALUES.
                cursor.execute(row["query_text"], skip_parsing=True)
                t2 = time.time()
            request_time = t2 - t1

            if "system" not in row["query_id"]:
                request_times[row["query_id"]] = request_time

            if start_time is None and "system" not in row["query_id"]:
                start_time = t1
        cursor.execute("set query_label=''")
    wall_clock_time = time.time() - start_time

    # Give engine_query_history time to populate
    time.sleep(10)

    sql = f"""\
        WITH modified_history as (
            select
            CASE WHEN query_label like 'b64-%' THEN
                CONVERT_FROM(DECODE(regexp_extract(query_label, 'b64-(.*)', '', 1), 'BASE64'), 'utf8')
            ELSE
                ''
            END as query_label_decoded,
            DATE_DIFF('microsecond', submitted_time, end_time) as duration_us_2,
            * exclude (query_label, duration_us)
            from information_schema.engine_query_history
        )
        select
            trim('"' from JSON_EXTRACT(query_label_decoded, '/sql_id', 'JSONPointer')) as sql_id,
            duration_us_2/1000000.0 as "server duration, s"
        from modified_history
        where
            JSON_EXTRACT(query_label_decoded, '/rid', 'JSONPointer') = '"{run_id}"'
            and JSON_EXTRACT(query_label_decoded, '/sql_id', 'JSONPointer') not like '"system%'
            and status = 'ENDED_SUCCESSFULLY'
            and query_text not ilike 'select 1'
        order by start_time"""
    cursor.execute(sql)
    results = cursor.fetchall()

    if len(results) == 0:
        print("ERROR: No queries were executed successfully.")
        cursor.close()
        connection.close()
        return

    def round(x): return np.round(x, 3)

    results_with_client_time = ((r[0], round(r[1]), round(request_times[r[0]])) for r in results)
    df_query_history = pd.DataFrame(results_with_client_time, columns=["sql_id", "server duration, s", "client duration, s"])
    print()
    print(df_query_history.to_markdown(index=False))
    print()

    print(f"Wall clock test duration: {wall_clock_time:.2f} seconds")

    values = [result[1] for result in results]
    server_sum = round(np.sum(values))
    server_mean = round(np.mean(values))
    server_geomean = round(np.exp(np.mean(np.log(values))))
    server_median = round(np.median(values))
    server_p95 = round(np.percentile(values, 95))

    values = [request_times[sql_id] for sql_id in df_query_history["sql_id"] if "system" not in sql_id and "select 1" not in sql_id and sql_id in request_times]
    client_sum = round(np.sum(values))
    client_mean = round(np.mean(values))
    client_geomean = round(np.exp(np.mean(np.log(values))))
    client_median = round(np.median(values))
    client_p95 = round(np.percentile(values, 95))

    df_query_history = pd.DataFrame(results_with_client_time, columns=["", "server durations, s", "client durations, s"])
    df_query_history.loc[1] = ["sum", server_sum, client_sum]
    df_query_history.loc[2] = ["mean", server_mean, client_mean]
    df_query_history.loc[3] = ["geometric mean", server_geomean, client_geomean]
    df_query_history.loc[4] = ["median", server_median, client_median]
    df_query_history.loc[5] = ["p95", server_p95, client_p95]

    print()
    print(df_query_history.to_markdown(index=False))

    cursor.close()
    connection.close()


def check_env_variable(var_name):
    value = os.environ.get(var_name)
    if value is None:
        print(f"Environment variable '{var_name}' is not set.")
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fireNEWT benchmark power run")
    parser.add_argument(
        "--query_history",
        type=str,
        help="Optional path to query history file",
        default="../SQL/queries/firenewt_1tb_powerrun.csv",
    )
    parser.add_argument(
        "--raw-http-queries",
        action=argparse.BooleanOptionalAction,
        help="If set, uses a lightweight raw HTTP query instead of the python SDK."
    )
    args = parser.parse_args()
    env_variables_set = True
    env_variables_set = env_variables_set and check_env_variable("FB_CLIENT_ID")
    env_variables_set = env_variables_set and check_env_variable("FB_CLIENT_SECRET")
    env_variables_set = env_variables_set and check_env_variable("FB_ACCOUNT")
    env_variables_set = env_variables_set and check_env_variable("FB_ENGINE")
    env_variables_set = env_variables_set and check_env_variable("FB_DATABASE")

    if env_variables_set:
        client_id=os.environ["FB_CLIENT_ID"]
        client_secret=os.environ["FB_CLIENT_SECRET"]
        account_name=os.environ["FB_ACCOUNT"]
        engine_name=os.environ["FB_ENGINE"]
        database=os.environ["FB_DATABASE"]
        api_endpoint=os.environ.get("FB_API")
        region=os.environ.get("FB_REGION").strip() if os.environ.get("FB_REGION") else None
        if api_endpoint is None:
            api_endpoint = "api.app.firebolt.io"
        run_powerrun(args.query_history, client_id, client_secret, account_name, engine_name, database, api_endpoint,
                     region=region, raw_http_query=args.raw_http_queries)
