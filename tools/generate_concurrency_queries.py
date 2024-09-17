import firebolt.db
from firebolt.client.auth import ClientCredentials
import os
import csv

def main():
    conn = firebolt.db.connect(
            auth=ClientCredentials(
                client_id=os.environ["FB_CLIENT_ID"],
                client_secret=os.environ["FB_CLIENT_SECRET"],
            ),
            account_name=os.environ["FB_ACCOUNT"],
            engine_name=os.environ["FB_ENGINE"],
            database=os.environ["FB_DATABASE"],
            api_endpoint=os.environ["FB_API"],
        )
    cursor = conn.cursor()

    query = """select    	
                    DATE_ADD('second', (id/200)::int, '2023-01-01 11:00:41'::TIMESTAMP) as query_start_ts,
                    DATE_ADD('second', (id/200)::int, '2023-01-01 11:00:41'::TIMESTAMP) as query_end_ts,
                    'c_app_q3_'||id as query_id,
                    query_text
            from (select 
                        (row_number() over ()) id,
                        'SELECT sourceip, searchword, sum(duration) as total_duration FROM uservisits WHERE visitdate = '''|| visitdate ||''' and destinationurl = '''|| destinationurl ||''' group by sourceip, searchword;' query_text
                    from (select distinct visitdate, destinationurl
                        from (select visitdate, destinationurl
                            from uservisits
                            where STRPOS(destinationurl,'''') = 0
                            order by sourceip
                            limit 4010000)))
            limit 4000000"""
    cursor.execute(query)
    chunk_size = 200000  # Number of rows per file
    file_count = 0

    data = cursor.fetchmany(chunk_size)
    column_names = [description[0] for description in cursor.description]    
    while data:                
        output_file = f"firenewt_qps_{file_count}.csv"        
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(data)        
        file_count += 1
        print(f"Written {len(data)} rows to {output_file}")
        data = cursor.fetchmany(chunk_size)

    print("Finished writing all CSV files.")

main()
