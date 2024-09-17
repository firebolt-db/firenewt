# FireNEWT: Nexus for Efficient Workload Testing
Welcome to FireNEWT (Nexus for Efficient Workload Testing), a comprehensive repository of queries and tests inspired by real-world data application production workloads. While industry-standard benchmarks like TPC-H have traditionally been used to gauge database performance, they often fall short in capturing the intricate complexities and unique demands found in actual production environments. To bridge this gap, we developed FireNEWT—a benchmark designed to offer a more accurate and representative measure of the challenges faced in modern data environments.

This repository serves as a valuable toolkit for developers, database administrators, and researchers, providing the means to evaluate and enhance system performance under realistic operational loads. By focusing on real-world patterns, FireNEWT offers a practical approach to workload testing, ensuring that database systems are tested against the kinds of demands they will encounter in production.

## About This Repository
This repository contains a collection of benchmarks designed to simulate production workload patterns. These benchmarks are useful for evaluating the performance of database systems.

## Getting Started

### Prerequisites

Before running any benchmarks, ensure you have the following:

- Git (for cloning the repository)
- Appropriate database management system (e.g., Firebolt, PostgreSQL) is configured

#### Firebolt Prerequisites

Specifically for Firebolt, you will need to have the following:

- A Firebolt service account. You will need to provide the service account ID and secret to the benchmarking scripts for them to connect to and authenticate with the Firebolt API.
  See the relevent [documentation](https://docs.firebolt.io/Guides/managing-your-organization/service-accounts.html) for more information about service accounts.
- A Firebolt user associated with the service account, to assign the appropriate roles to access Firebolt resources such as databases and engines.
  See the relevent [documentation](https://docs.firebolt.io/Guides/managing-your-organization/managing-users.html) for more information about users.
- A Firebolt database that the user has read and write access to.
- A Firebolt engine that the user can use. To test various engine configurations, you will either need existing engines with those configurations, or the ability to create or modify engines and operate them.
  See the relevent [documentation](https://docs.firebolt.io/Guides/operate-engines/operate-engines.html) for more information about engines.

### Installation

Clone the repository to your local machine using:

```bash
git clone https://github.com/firebolt-db/firenewt.git
```

Navigate to the cloned directory:

```bash
cd firenewt
```

Install necessary dependencies:
```bash
cd ./tools
pip install -r requirements.txt
```

### Creating a database

Data are available on s3 but also can be generated via sql scripts.

#### Load Data from S3
Create a Firebolt database and engine, create tables and load data for 1TB or 100GB dataset via `data/ingest_1tb_s3.sql` or `data/ingest_100gb_s3.sql`.

#### Generate Data
Create a Firebolt database and engine, create tables and load data for 1TB or 100GB dataset via `data/firenewt_1tb_data_generator.sql` or `data/ingest_100gb_s3.sql`. 

Queries for new data sets can also be generated via `tools/generate_powerrun_queries.py` and `tools/generate_concurrency_queries.py`.

### Running Benchmarks

#### General Benchmarking Information

Scripts typically require that most environment variables that follow are set to the appropriate value.

As mentioned before, the benchmarker requires a Firebolt service account and associated user.
The benchmarker authenticates with the API using the service account ID and secret, provided as environment variables:
```bash
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
```

The following environment variables are required to specify the Firebolt engine, database, and the account they are in
(not to be confused with a "service account", which is a different concept used for authentication), by name:
```bash 
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
```

The API is specified using the following environment variable, which should likely always be the value shown:
```bash
export FB_API=api.app.firebolt.io
```

Scripts that access data in Firebolt public S3 buckets, e.g. to ingest the base tables, require the following
environment variable be set to the name of the Firebolt AWS region the engine is running in so that the correct regional
bucket is used, e.g. `us-east-1`:
```bash
export FB_REGION=...
```

#### Running Concurrency Benchmarks

To run a specific benchmark, download relevant query history scripts from `s3://firebolt-benchmarks-requester-pays-us-east-1/firenewt/1tb/sql/queries` folder and execute the
corresponding script `tools/run_firenewt_concurrent_qps.py` with the desired concurrency level and the paths to the
query history files as arguments.

**1 cluster 1 node type L engine high QPS benchmark**
```bash 
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io

cd tools
python run_firenewt_concurrent_qps.py --concurrency 200 firenewt_1tb_qps_0.csv firenewt_1tb_qps_1.csv firenewt_1tb_qps_2.csv firenewt_1tb_qps_3.csv                     
```

**10 clusters 1 type L engine high QPS benchmark**
```bash 
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io

cd tools
python run_firenewt_concurrent_qps.py --concurrency 400 firenewt_1tb_qps_0.csv firenewt_1tb_qps_0.csv firenewt_1tb_qps_1.csv firenewt_1tb_qps_2.csv firenewt_1tb_qps_3.csv firenewt_1tb_qps_4.csv firenewt_1tb_qps_5.csv firenewt_1tb_qps_6.csv firenewt_1tb_qps_7.csv firenewt_1tb_qps_8.csv firenewt_1tb_qps_9.csv firenewt_1tb_qps_10.csv firenewt_1tb_qps_11.csv firenewt_1tb_qps_12.csv firenewt_1tb_qps_13.csv firenewt_1tb_qps_14.csv firenewt_1tb_qps_15.csv firenewt_1tb_qps_16.csv firenewt_1tb_qps_17.csv firenewt_1tb_qps_18.csv firenewt_1tb_qps_19.csv
```

#### Running the Power Run Benchmark

The default parameters for `tools/run_firenewt_powerrun.py` will run the appropriate queries against the specified engine.

```bash 
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io

cd tools
python run_firenewt_powerrun.py
```

#### Running the Bulk Ingest Benchmarks

Find the desired query history scenario CSV file in the `SQL/bulk_ingestion` folder. Each of the three scenarios runs a
`COPY FROM` query against a set of files of the specified file format that exist in a Firebolt public S3 bucket. Each
of the `bi_1b_*.csv` files is a different scenario loading the same underlying 1 TB of data, but with different file
formats.

To test different engine configurations, you will need to configure the engine externally, e.g. by issuing SQL commands
from the Firebolt web UI.

You will need to specify the `FB_REGION` environment variable with the same region the engine is running in,
e.g. `us-east-1`, so that the COPY FROM commands target the correct regional bucket.

The query history scenarios can be run with the same command used for power runs, `run_firenewt_powerrun.py`, which is
basically a tool that issues queries from the history CSV files sequentially and gathers statistics.

For example:
```bash
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io
export FB_REGION=us-east-1

cd tools
python run_firenewt_powerrun.py --query_history=../SQL/bulk_ingestion/bi_1tb_snappy_parquet.csv
```

#### Running the Trickle Ingestion / DML Benchmarks

Similar to running bulk ingest scenarios, find the desired query history scenario CSV file in the 
`SQL/trickle_ingestion` folder. There are various scenarios for INSERT, UPDATE, and DELETE operations. Each scenario
repeats the same type and size of DML operation 100 times, with different data for each. E.g., the scenario
`insert_10r_100q.csv` issues 100 distinct INSERT queries, each inserting 10 rows into a table. The UPDATE and DELETE
scenarios like `delete_1r_100q.csv` each issue 100 distinct queries that update or delete 1 row each from the table.

You will need to specify the `FB_REGION` environment variable with the same region the engine is running in,
e.g. `us-east-1`, so that the COPY FROM commands target the correct regional bucket when ingesting a fresh copy of
the base table.

Again, use the power run script, `run_firenewt_powerrun.py`, to run the DML scenarios from the query history CSV files
and collect statistics.

For example:
```bash
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io
export FB_REGION=us-east-1

cd tools
python run_firenewt_powerrun.py --query_history=../SQL/trickle_ingestion/insert_10r_100q.csv
```

## Repository Structure

- **/queries/**: Contains all benchmark queries, organized by database type.
- **/data/**: Includes scripts and links to datasets required for running benchmarks.
- **/results/**: Stores example results and performance metrics.
- **/scripts/**: Automation and utility scripts to facilitate benchmarking processes.
- **/docs/**: Additional documentation on benchmark methodology and detailed guides.

## Contributing


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE) file for details.
