import argparse
import base64
import pandas as pd
import json
import firebolt.db
from firebolt.client.auth import ClientCredentials
import os
import random
import string
import time

import httpx

def run_powerrun(query_history, client_id, client_secret, account_name, engine_name, database, api_endpoint, raw_http_query=False):
    df = pd.read_csv(query_history)
    df.sort_values("query_start_ts", inplace=True)
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
    print(f" Run id is {run_id}")

    with httpx.Client(timeout=httpx.Timeout(7200.0)) as client:
        for index, row in df.iterrows():                
            query_label_decoded = json.dumps({"rid": run_id, "sql_id": row["query_id"]})
            query_label = f'b64-{base64.b64encode(query_label_decoded.encode()).decode()}'

            if raw_http_query:
                engine_url = connection.engine_url + f"?advanced_mode=1&result_cache_max_bytes_item=0&database={database}&engine={engine_name}&query_label={query_label}"
                t1 = time.time()
                response = client.post(engine_url, data=row["query_text"], headers=headers)
            else:
                cursor.execute(f'set query_label={query_label}')
                t1 = time.time()
                # skip_parsing significantly speeds up the client side part of this query. This can greatly affect
                # certain cases, such as INSERT queries with 1000 literal row VALUES.
                cursor.execute(row["query_text"], skip_parsing=True)

            if start_time is None and "system" not in row["query_id"]:
                start_time = t1
        cursor.execute("set query_label=''")
    wall_clock_time = time.time() - start_time

    # Give engine_query_history time to populate
    time.sleep(10)

    sql = f"""\
        WITH augmented_history as (
            select
            CASE WHEN query_label like 'b64-%' THEN
                CONVERT_FROM(DECODE(regexp_extract(query_label, 'b64-(.*)', '', 1), 'BASE64'), 'utf8')
            ELSE
                ''
            END as query_label_decoded,
            *
            from information_schema.engine_query_history
        )
        select
            trim('"' from JSON_EXTRACT(query_label_decoded, '/sql_id', 'JSONPointer')) as sql_id,
            round(duration_us/1000000.0,3) as "duration, s"
        from augmented_history
        where
            JSON_EXTRACT(query_label_decoded, '/rid', 'JSONPointer') = '"{run_id}"'
            and JSON_EXTRACT(query_label_decoded, '/sql_id', 'JSONPointer') not like '"system%'
            and status = 'ENDED_SUCCESSFULLY'
            and query_text not ilike 'select 1'
        order by start_time"""
    cursor.execute(sql)
    results = cursor.fetchall()
    df_query_history = pd.DataFrame(results, columns=["sql_id", "duration"])
    print(df_query_history.to_markdown(index=False))
    print(f"Wall clock test duration: {wall_clock_time:.2f} seconds")
    sql = f"""
        WITH augmented_history as (
            select
            CASE WHEN query_label like 'b64-%' THEN
                CONVERT_FROM(DECODE(regexp_extract(query_label, 'b64-(.*)', '', 1), 'BASE64'), 'utf8')
            ELSE
                ''
            END as query_label_decoded,
            *
            from information_schema.engine_query_history
        )
        select
            round(SUM(duration_us/1000000.0),3) as total_duration,
            round(POW(2.718281828459045,(SUM(LN(duration_us/1000000.0)) / COUNT(*))),3) AS geometric_mean
        from augmented_history
        where
            JSON_EXTRACT(query_label_decoded, '/rid', 'JSONPointer') = '"{run_id}"'
            and JSON_EXTRACT(query_label_decoded, '/sql_id', 'JSONPointer') not like '"system%'
            and status = 'ENDED_SUCCESSFULLY'
            and query_text not ilike 'select 1'
        group by all"""
    cursor.execute(sql)
    results = cursor.fetchall()
    print(f"Total duration for all queries: {results[0][0]:.2f} seconds")
    print(f"Geometric mean of query durations: {results[0][1]:.2f} seconds")
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
        if api_endpoint is None:
            api_endpoint = "api.app.firebolt.io"
        run_powerrun(args.query_history, client_id, client_secret, account_name, engine_name, database, api_endpoint,
                     raw_http_query=args.raw_http_queries)
