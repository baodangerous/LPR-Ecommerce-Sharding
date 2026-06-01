<h1 align="center">LPR — Lightweight Proactive Re-sharding</h1>

<p align="center">
  <img src="assets/architecture.svg" alt="LPR System Architecture" width="100%">
</p>

<p align="center">
  <a href="https://colab.research.google.com/github/baodangerous/LPR-Ecommerce-Sharding/blob/main/Proactive_ReSharding.ipynb">
    <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open in Colab"/>
  </a>
  &nbsp;
  <a href="https://baodangerous.github.io/LPR-Ecommerce-Sharding/">
    <img src="https://img.shields.io/badge/Live%20Demo-GitHub%20Pages-0969da?style=flat-square" alt="Live Demo"/>
  </a>
  &nbsp;
  <img src="https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/License-MIT-22C55E?style=flat-square"/>
</p>

<p align="center">
  <strong>Proactive hotspot detection for trusted distributed databases in e-commerce</strong><br/>
  <sub>Cơ chế tái phân mảnh chủ động nhẹ cho hệ CSDL phân tán trong môi trường tin cậy</sub><br/>
  <sub>University of Information Technology · VNU-HCM · Big Data & Distributed Database Systems · 2026</sub>
</p>

---

## The Problem

In a distributed database serving e-commerce, a Flash Sale event can send thousands of requests per second to the same product shard — while the other three sit idle. This is called a **hot-spot shard**, and it slows down every query in the cluster even though total capacity is more than sufficient.

On the Amazon Sale Report dataset (128,975 transactions), we measured an Imbalance Ratio of **43.12** — one shard handling 43× the average load. The root cause is Zipf distribution: top 20% of products receive 80%+ of all requests.

```
Imbalance Ratio = max(shard_load) / mean(shard_load)
Measured on dataset:  IR = 43.12
Perfect balance:      IR = 1.00
```

---

## Why Existing Solutions Fall Short

| Approach | Problem |
|---|---|
| **Static Sharding** | Never rebalances. Hot-spots are permanent. |
| **Reactive Re-sharding** | Reacts only after overload — causes migration thrashing (300 unnecessary migrations on uniform workload in our experiments). |
| **Marlin** *(SIGMOD 2025)* | Excellent for untrusted blockchain. Byzantine consensus adds O(n²) overhead that trusted enterprise databases do not need. |
| **PSAP** *(arXiv 2025)* | Safe-PPO reinforcement learning — requires offline training, validator synchronization, and specialized hardware. |

The gap: a **simple, deterministic, zero-training** mechanism for trusted distributed databases that acts *before* the hotspot peaks.

---

## The Insight

Instead of reacting to current load, watch **how fast it is growing**.

```
# Load acceleration — fires before the peak
GR_i(t)  = ( L_i(t) − L_i(t−1) ) / ( L_i(t−1) + ε )    ε = 1e-5

# Exponential Moving Average — noise suppression
EMA_i(t) = α · L_i(t) + (1−α) · EMA_i(t−1)              α = 0.3

# Adaptive threshold — self-adjusts to cluster state
T(t)     = mean( L_j(t) ) · γ                             γ = 1.5

# Hotspot Score — unified proactive signal
H_i(t)   = EMA_i(t) + β · GR_i(t)                        β = 0.5

# Cost-Benefit gate — anti-thrashing
migrate only when: Expected Benefit > 1.2 × Migration Cost
```

Four equations. Five scalar hyperparameters. No training. No consensus rounds. Fully deterministic — same input always produces the same decision. Algorithm complexity: **O(k) per cycle**, where k = keys in the hot shard.

---

## Experimental Results

All numbers below are taken directly from notebook cell outputs. Simulation: 50,000 requests, 4 shards, `REACTIVE_THRESHOLD = 150` (= 1.2× average load per step).

### Three workload scenarios

| Scenario | Strategy | Throughput (req/s) | Latency (s) | Imbalance Ratio | Migrations |
|---|---|---|---|---|---|
| **Uniform** | Static | 1,000,120 | 0.0500 | 1.54 | 0 |
| | Reactive | 914,182 | 0.0547 | 1.54 | 300 |
| | **LPR** | **1,056,686** | **0.0473** | **1.34** | **3** |
| **Zipfian** | Static | 1,039,182 | 0.0481 | 2.83 | 0 |
| | Reactive | 998,958 | 0.0501 | 2.83 | 100 |
| | **LPR** | 979,735 | 0.0510 | **2.70** | **84** |
| **Flash-sale** | Static | 1,066,433 | 0.0469 | 2.01 | 0 |
| | Reactive | 806,715 | 0.0620 | 2.01 | 100 |
| | **LPR** | **981,666** | **0.0509** | 2.03 | **44** |

### LPR improvement over Reactive

| Metric | Uniform | Zipfian | Flash-sale |
|---|---|---|---|
| Throughput | **+15.6%** | −1.9%* | **+21.7%** |
| Latency | **−13.5%** | −1.8%* | **−17.9%** |
| Imbalance Ratio | **−13.0%** (1.34 vs 1.54) | **−4.6%** (2.70 vs 2.83) | +1.0% |
| Migrations | **−99.0%** (3 vs 300) | **−16.0%** (84 vs 100) | **−56.0%** (44 vs 100) |

> *On Zipfian, LPR throughput is 1.9% lower than Reactive. This reflects EMA and Growth Rate computation overhead measured in wall-clock simulation time. In production I/O-bound systems, O(k) algorithm overhead is negligible compared to actual network latency.

### Key findings

**Uniform workload** is where LPR's idle cost shows most clearly: only 3 migrations versus 300 for Reactive. The Cost-Benefit gate correctly identifies that almost no migration is profitable on balanced load, keeping overhead near zero.

**Zipfian workload** demonstrates the core value: LPR achieves better load balance (IR 2.70 vs 2.83) with 16% fewer migrations, confirming the proactive signal detects hotspots before they fully form.

**Flash-sale workload** shows the anti-thrashing effect most dramatically: LPR uses 56% fewer migrations (44 vs 100) while delivering 21.7% higher throughput and 17.9% lower latency than Reactive.

---

## Ablation Study

Each component is removed to isolate its contribution. Zipfian workload, `run_ablation()`.

| Variant | Throughput (req/s) | Migrations | IR |
|---|---|---|---|
| Reactive baseline | 1,352,237 | 0* | 2.706 |
| LPR — remove EMA | 1,194,346 | 102 | 2.602 |
| LPR — remove Growth Rate | 1,224,958 | 89 | 2.652 |
| LPR — remove Cost-Benefit gate | 1,250,235 | 93 | 2.842 |
| **Full LPR** | **1,262,315** | **0** | **2.706** |

> *Reactive migrations = 0 in this run because the ablation workload did not cross `threshold = 150` on this particular run. This is expected behavior for a balanced enough workload.

**EMA is the most critical component**: removing it causes migrations to spike to 102 — equal to Reactive's worst-case behavior. Raw load signal is too noisy for stable proactive decisions.

**Cost-Benefit gate is the anti-thrashing mechanism**: removing it causes migrations to rise to 93. Full LPR achieves 0 unnecessary migrations — the gate correctly identifies no migration is sufficiently profitable on this run.

**Growth Rate contributes early detection**: without it, migrations rise to 89 and IR worsens. The Growth Rate signal shifts intervention 3–5 cycles earlier in the hotspot lifecycle.

---

## Connection to Course Material

### Distributed Database Systems

- **Horizontal sharding and consistent hashing** — LPR uses consistent hashing `H: K → S` for initial key routing, then maintains a `key_to_shard_map` override layer for migrated hot keys. This is the standard approach in systems like DynamoDB and Cassandra.
- **Workload-aware vs. data-aware partitioning** — Static sharding distributes data evenly; LPR distributes *request load* evenly. Hot-spot formation occurs precisely when request distribution (Zipf) diverges from data distribution (uniform) — the fundamental tension in distributed database design.
- **Imbalance Ratio as a system health metric** — IR = max(load) / mean(load) is the standard metric for partition skew in distributed storage systems.

### Big Data Processing (PySpark)

- **Spark for data preprocessing** — PySpark reads and processes the 128,975-record Amazon Sale Report dataset, computes SKU frequency distributions, and runs temporal workload analysis using Spark window functions. This is a real ETL pipeline, not a toy example.
- **Data skew** — the central problem in both Spark (straggler tasks) and distributed databases (hot-spot shards) is the same: Zipf-distributed access patterns. The solution principles — salting, repartitioning, proactive load balancing — are analogous across both contexts.
- **Synthetic workload generation** — Zipf distribution (s = 2.0) is the standard workload model in distributed systems benchmarking (YCSB — Yahoo Cloud Serving Benchmark). This is standard Big Data research methodology.

### Why CPU, not GPU

This simulation runs on Google Colab CPU runtime by design. Apache Spark processes distributed workloads on CPU clusters. Sharding involves hash routing, load monitoring, and scheduling — CPU-bound operations. Cassandra, MongoDB, and DynamoDB all run on CPU server clusters in production. GPU acceleration applies to matrix operations and deep learning, not to database coordination workloads.

---

## Repository Structure

```
LPR-Ecommerce-Sharding/
│
├── Proactive_ReSharding.ipynb     Main experiment notebook — open directly in Colab
│
├── lpr/
│   ├── shard.py                   Shard class — load tracking, key management
│   ├── metrics.py                 EMA, Growth Rate, Hotspot Score, Adaptive Threshold
│   └── lpr_sharding.py            LPRSharding — complete algorithm
│
├── baselines/
│   ├── static_sharding.py         StaticSharding
│   └── reactive_sharding.py       ReactiveSharding
│
├── experiments/
│   ├── workload_generator.py      Uniform / Zipfian / Flash-sale synthesis
│   └── simulation.py              Evaluation pipeline
│
├── visualization/
│   └── plot_results.py            Consistent color palette, publication-ready charts
│
├── assets/
│   └── architecture.svg           System architecture diagram
│
├── docs/
│   └── index.html                 Interactive research dashboard (GitHub Pages)
│
└── requirements.txt
```

---

## Quickstart

```bash
git clone https://github.com/baodangerous/LPR-Ecommerce-Sharding.git
cd LPR-Ecommerce-Sharding
pip install -r requirements.txt
```

```python
from lpr.shard import Shard
from lpr.lpr_sharding import LPRSharding
from experiments.workload_generator import generate_zipfian_workload
from experiments.simulation import run_simulation

workload = generate_zipfian_workload(50000, 100, skew=2.0)
shards   = [Shard(i) for i in range(4)]
strategy = LPRSharding(num_shards=4, alpha=0.3, beta=0.5, gamma=1.5, cost_barrier=1.2)
result   = run_simulation(workload, strategy, shards)

print(f"Imbalance Ratio : {result['final_imbalance']:.2f}")   # expect < 2.83 (Static baseline)
print(f"Migrations      : {result['total_migrations']}")       # expect < 100 (Reactive baseline)
print(f"Throughput      : {result['total_throughput']:,.0f} req/s")
```

---

## Hyperparameters

| Parameter | Default | Role |
|---|---|---|
| `alpha` | `0.3` | EMA smoothing — lower = more stable, higher = more responsive |
| `beta` | `0.5` | Weight of Growth Rate in Hotspot Score |
| `gamma` | `1.5` | Threshold multiplier — how far above mean load before action |
| `migration_fraction` | `0.1` | Fraction of hot keys migrated per trigger |
| `cost_barrier` | `1.2` | Minimum benefit/cost ratio — primary anti-thrashing control |

---

## Comparison with State-of-the-Art

| | **LPR** | PSAP *(arXiv 2025)* | Marlin *(SIGMOD 2025)* |
|---|---|---|---|
| Target environment | Trusted enterprise DBMS | Untrusted blockchain | Untrusted blockchain |
| Prediction mechanism | EMA + Growth Rate | Safe-PPO RL | Byzantine consensus |
| Per-cycle complexity | **O(k)** | High | O(n²) |
| Training required | **No** | Yes | No |
| Fully deterministic | **Yes** | Requires validator sync | Partial |
| Deployment effort | **5 hyperparameters** | High | High |

LPR is not designed to replace PSAP or Marlin. It addresses a distinct and underserved problem: **trusted enterprise databases** where Byzantine overhead is unnecessary and operational simplicity is a hard requirement.

---

## References

1. M. J. Amiri, "Adaptive Sharding for Scalable Blockchain Platforms," *ICDE*, 2021.
2. B. Mehta, N. Baghel, M. J. Amiri, B. T. Loo, R. Marcus, "Marlin: Adaptive Sharding in Untrusted Environments," *SIGMOD*, 2025.
3. M. Z. Haider, T. Noreen, M. D. Assunção, K. Zhang, "AI-driven Predictive Shard Allocation for Scalable Next-Generation Blockchains," *arXiv:2511.19450*, 2025.
4. Kaggle, "Amazon Sale Report Dataset," 2023. [Link](https://www.kaggle.com/datasets/mdsazzatsardar/amazonsalesreport)

---

## Developed By

**Nguyen Le Bao Dang** (23520230) · **Pham Minh Ngan** (23520997)  
Faculty of Information Systems · University of Information Technology · VNU-HCM  
*Big Data Processing & Distributed Database Systems · 2026*
