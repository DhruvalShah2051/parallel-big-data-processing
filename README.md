# Parallel Big Data Processing

Big data analytics on a 2.9GB, 3-million-row book ratings dataset using three parallelism techniques: multi-threading, multi-processing, and MPI. Each analytical query is implemented independently across all three techniques, with performance benchmarking across varying worker counts — developed as part of COMP6231 (Distributed Systems Design) at Concordia University.

## Overview

The goal is to demonstrate the trade-offs between concurrency and parallelism strategies when processing large-scale datasets. Dataset rows are distributed evenly across workers to ensure balanced workload distribution, with results aggregated by a coordinator process.

## Dataset

- **Source:** Book ratings CSV (~2.9 GB, 3M rows)
- **Fields:** Book title, user name, recommendation score (RScore), price, and more
- A single user may have multiple names; a single book may have multiple titles

## Analytical Queries

| Query | Description |
|---|---|
| Q1 | Count reviews with a recommendation score < 4 |
| Q2 | Count distinct books with average RScore = 5 and price = 1 |
| Q3 | Find the user who reviewed the most books with an overall average RScore = 5 |
| Q4 | Top 10 highest-priced books with average RScore < 3, sorted by price descending |

## Parallelism Techniques

Each query is implemented using all three techniques:

- **T1 — Multi-threading** (`threading` module): concurrent execution within a single process; effective for I/O-bound workloads
- **T2 — Multi-processing** (`multiprocessing` module): true parallelism via separate processes; bypasses Python's GIL for CPU-bound tasks
- **T3 — MPI** (`mpi4py`): distributed message-passing across multiple workers; master node distributes chunks and aggregates results

## Performance Analysis

- **A1:** Varies MPI worker count from 1–10 for Q2; reports average time over 3 runs per configuration
- **A2:** Compares all three techniques for Q3 with worker/thread/process count varying from 1–10; returns timing dictionary `{MT: {...}, MP: {...}, MPI: {...}}`

## Repository Structure

```
├── implementation/
│   ├── q1/   # t1.py (threading), t2.py (multiprocessing), t3.py (MPI)
│   ├── q2/
│   ├── q3/
│   └── q4/
└── analysis/
    ├── A1.py  # MPI scaling analysis for Q2
    └── A2.py  # Cross-technique performance comparison for Q3
```

## Technical Stack

- Python 3.9+
- `threading`, `multiprocessing` — standard library parallelism
- `mpi4py` — MPI-based distributed processing
- `pandas` — data manipulation and aggregation

## Notes

- Developed as part of COMP6231 Fall 2024 at Concordia University
- Multi-threading and multi-processing implementations fully functional across all 4 queries
- Dataset not included due to size; available on [Kaggle](https://www.kaggle.com)
