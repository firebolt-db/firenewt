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

### Installation

Clone the repository to your local machine using:

```bash
git clone https://github.com/<yourusername>/firebolt-benchmark-suite.git
```

Navigate to the cloned directory:

```bash
cd firebolt-benchmark-suite
```

Install necessary dependencies:
```bash
cd ./tools
pip install -r requirements.txt
```


### Running Benchmarks

To run a specific benchmark, navigate to its directory under the `/queries` folder and execute the corresponding script. For example:

1 cluster 1 node type L engine high QPS benchmark
```bash 
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io

cd tools
python run_firenewt_concurrent_qps.py --concurrency 200 ../SQL/queries/firenewt_1tb_qps_0.csv ../SQL/queries/firenewt_1tb_qps_1.csv ../SQL/queries/firenewt_1tb_qps_2.csv ../SQL/queries/firenewt_1tb_qps_3.csv
```

10 clusters 1 type L engine high QPS benchmark
```bash 
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io

cd tools
python run_firenewt_concurrent_qps.py --concurrency 400 ../SQL/queries/firenewt_1tb_qps_0.csv ../SQL/queries/firenewt_1tb_qps_0.csv ../SQL/queries/firenewt_1tb_qps_1.csv ../SQL/queries/firenewt_1tb_qps_2.csv ../SQL/queries/firenewt_1tb_qps_3.csv ../SQL/queries/firenewt_1tb_qps_4.csv ../SQL/queries/firenewt_1tb_qps_5.csv ../SQL/queries/firenewt_1tb_qps_6.csv ../SQL/queries/firenewt_1tb_qps_7.csv ../SQL/queries/firenewt_1tb_qps_8.csv ../SQL/queries/firenewt_1tb_qps_9.csv ../SQL/queries/firenewt_1tb_qps_10.csv ../SQL/queries/firenewt_1tb_qps_11.csv ../SQL/queries/firenewt_1tb_qps_12.csv ../SQL/queries/firenewt_1tb_qps_13.csv ../SQL/queries/firenewt_1tb_qps_14.csv ../SQL/queries/firenewt_1tb_qps_15.csv ../SQL/queries/firenewt_1tb_qps_16.csv ../SQL/queries/firenewt_1tb_qps_17.csv ../SQL/queries/firenewt_1tb_qps_18.csv ../SQL/queries/firenewt_1tb_qps_19.csv
```


```bash 
export FB_CLIENT_ID=...
export FB_CLIENT_SECRET=...
export FB_ACCOUNT=...
export FB_ENGINE=...
export FB_DATABASE=...
export FB_API=api.app.firebolt.io

cd tools
python run_firenewt_powerrun.py

Run id is powerrun_950516_date_2024_09_10_time_15_08_09
| sql_id   |   duration |
|:---------|-----------:|
| app_q1   |      0.076 |
| app_q2   |      0.108 |
| app_q3   |      0.051 |
| app_q4   |      0.036 |
| app_q5   |      0.063 |
| app_q6   |      0.033 |
| app_q7   |      0.057 |
| app_q8   |      0.047 |
| app_q9   |      0.22  |
| app_q10  |      1.774 |
| app_q11  |      0.051 |
| app_q12  |      0.098 |
| app_q13  |      0.773 |
| app_q15  |      0.049 |
| app_q16  |      0.058 |
| app_q22  |      0.795 |
Wall clock test duration: 11.09 seconds
Total duration for all queries: 4.29 seconds
Geometric mean of query durations: 0.11 seconds
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
