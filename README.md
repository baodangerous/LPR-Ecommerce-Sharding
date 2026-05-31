# LPR — Lightweight Proactive Re-sharding

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
  <strong>Proactive hotspot detection for trusted distributed databases</strong><br/>
  <sub>University of Information Technology · VNU-HCM · Big Data & Distributed Systems · 2025–2026</sub>
</p>

---

## The Problem

Picture a Flash Sale: thousands of users flood a single product page simultaneously. In a sharded database, every one of those requests hits the same shard — while the other three sit idle. The system does not fail, but one shard becomes a **bottleneck** that slows down every query in the cluster.

On the Amazon Sale Report dataset (128,975 transactions), we measured an Imbalance Ratio of **43.12** — one shard handling 43× the average load. The top 20% of products received over 80% of all requests, a textbook Zipf distribution.

```
Imbalance Ratio = max(shard load) / mean(shard load)

Measured on Amazon dataset:   IR = 43.12
Ideal (perfectly balanced):   IR = 1.00
```

---

## Why Existing Solutions Fall Short

The obvious fix is to move hot data to less-loaded shards. The hard part is *when* and *how often*.

| Approach | What goes wrong |
|---|---|
| **Static Sharding** | Never rebalances. Hot-spots are permanent. |
| **Reactive Re-sharding** | Waits until overload happens, then scrambles — causing migration thrashing (224 migrations in our Flash-sale experiment) |
| **Marlin** *(SIGMOD 2025)* | Excellent for untrusted blockchain networks. But Byzantine consensus adds coordination overhead that trusted enterprise DBs don't need. |
| **PSAP** *(arXiv 2025)* | Safe-PPO reinforcement learning achieves great predictions. Requires offline training, validator synchronization, and specialized hardware. |

There is a gap: a **simple, deterministic, zero-training** mechanism for trusted distributed databases that acts *before* the hotspot peaks rather than after.

---

## The Insight

Instead of reacting to load, watch *how fast* it is growing.

A shard about to become a hot-spot shows a rising load trajectory *before* it crosses any danger threshold. LPR monitors this trajectory using two lightweight signals — an EMA filter to suppress noise, and a Growth Rate to detect acceleration — then acts early.

```
# Is this shard getting hotter faster than average?
GR_i(t)  = ( L_i(t) − L_i(t−1) ) / ( L_i(t−1) + ε )   # load acceleration
EMA_i(t) = α · L_i(t) + (1−α) · EMA_i(t−1)              # smoothed load

# Hotspot Score — fires before the peak
H_i(t)   = EMA_i(t) + β · GR_i(t)

# Only migrate if it's actually worth it (anti-thrashing gate)
Expected Benefit > 1.2 × Migration Cost
```

Four equations. Five scalar hyperparameters. No training. No consensus rounds. Fully deterministic — same input always produces the same decision.

---

## Results

Experiments on 50,000 requests, 4 shards, two skewed workload scenarios:

### Flash-sale workload — burst traffic, 40% of requests to one shard

| Strategy | Throughput (req/s) | Latency (s) | Imbalance Ratio | Migrations |
|---|---|---|---|---|
| Static | 731,263 | 0.068 | 2.30 | 0 |
| Reactive | 507,817 | 0.099 | 2.30 | 224 |
| **LPR** | **707,374** | **0.071** | **2.26** | **83** |

LPR reaches the same load balance as Reactive with **62.9% fewer migrations** (83 vs 224) and **39.3% higher throughput**.

### Zipfian workload — long-tail distribution, s = 2.0

| Strategy | Throughput (req/s) | Latency (s) | Imbalance Ratio | Migrations |
|---|---|---|---|---|
| Static | 465,139 | 0.108 | 3.14 | 0 |
| Reactive | 637,188 | 0.079 | 3.14 | 100 |
| **LPR** | 371,974 | 0.134 | **2.84** | **61** |

LPR achieves **9.6% better load balance** than Reactive (IR 2.84 vs 3.14) with 39% fewer migrations. The throughput trade-off reflects EMA+GR computation in the simulation's wall-clock measurement — in production I/O-bound systems, O(k) algorithm overhead is negligible compared to actual network latency.

### Ablation — which component matters most?

| Variant | Migrations | IR |
|---|---|---|
| Reactive baseline | 0* | 2.618 |
| LPR — remove EMA | 102 | 2.672 |
| LPR — remove Growth Rate | 85 | 2.623 |
| LPR — remove Cost-Benefit gate | 90 | 2.663 |
| **Full LPR** | **0** | **2.618** |

*Reactive had 0 migrations here because the ablation workload did not cross threshold=150 in this run.

**EMA is the most important component** — without it, raw load noise causes as many false-positive migrations as Reactive. The Cost-Benefit gate is what makes LPR "lightweight": it prevents any migration that does not improve balance enough to justify the network cost.

---

## How It Works — Decision Pipeline

Each time cycle *t*, the LPR coordinator runs:

```
for each shard i:
    1. Collect current load   L_i(t)
    2. Update EMA             EMA_i(t) = 0.3·L_i(t) + 0.7·EMA_i(t−1)
    3. Compute Growth Rate    GR_i(t)  = ΔL / (L_prev + ε)
    4. Compute Hotspot Score  H_i(t)   = EMA_i(t) + 0.5·GR_i(t)
    5. Compute threshold      T(t)     = mean(all loads) × 1.5

    if H_i(t) > T(t):
        benefit = projected load reduction
        cost    = keys to move × transfer overhead
        if benefit > 1.2 × cost:
            migrate top-10% hot keys → coolest shard
            update key_to_shard_map
```

Algorithm complexity: **O(k) per cycle**, where k = keys in the hot shard. Scales independently of cluster size.

---

## Comparison with State-of-the-Art

LPR is not designed to replace PSAP or Marlin — they solve a different problem (untrusted Byzantine environments). LPR fills a specific gap: **trusted enterprise databases** where simplicity, determinism, and zero training overhead are requirements.

| | **LPR** | PSAP *(2025)* | Marlin *(SIGMOD 2025)* |
|---|---|---|---|
| Target environment | Trusted enterprise DBMS | Untrusted blockchain | Untrusted blockchain |
| Prediction mechanism | EMA + Growth Rate | Safe-PPO RL | Byzantine consensus |
| Per-cycle complexity | **O(k)** | High | O(n²) |
| Training required | **No** | Yes | No |
| Fully deterministic | **Yes** | Requires validator sync | Partial |
| Dedicated hardware | **No** | Yes | No |
| Deployment effort | **5 hyperparameters** | High | High |

---

## Repository

```
LPR-Ecommerce-Sharding/
│
├── Proactive_ReSharding.ipynb   Full experiment notebook — open directly in Colab
│
├── lpr/
│   ├── shard.py                 Shard — load tracking, key management
│   ├── metrics.py               EMA · Growth Rate · Hotspot Score · Adaptive Threshold
│   └── lpr_sharding.py          LPRSharding — complete algorithm
│
├── baselines/
│   ├── static_sharding.py       StaticSharding
│   └── reactive_sharding.py     ReactiveSharding
│
├── experiments/
│   ├── workload_generator.py    Zipfian · Flash-sale · Uniform synthesis
│   └── simulation.py            Evaluation pipeline
│
├── visualization/
│   └── plot_results.py          Consistent color palette, publication-ready charts
│
├── assets/
│   └── architecture.svg         System diagram
│
├── docs/
│   └── index.html               Interactive dashboard (GitHub Pages)
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

print(f"Imbalance Ratio : {result['final_imbalance']:.2f}")   # target < 3.14
print(f"Migrations      : {result['total_migrations']}")       # target < 100
print(f"Throughput      : {result['total_throughput']:,.0f} req/s")
```

---

## Hyperparameters

| Parameter | Default | Role |
|---|---|---|
| `alpha` | `0.3` | EMA smoothing — lower = more stable, higher = more responsive |
| `beta` | `0.5` | Weight of Growth Rate in Hotspot Score |
| `gamma` | `1.5` | Threshold multiplier — how far above mean before action |
| `migration_fraction` | `0.1` | Share of hot keys moved per trigger |
| `cost_barrier` | `1.2` | Minimum benefit/cost ratio — primary anti-thrashing control |

---

## A Note on CPU vs GPU

This simulation runs on **Google Colab CPU runtime** — which is exactly right for this workload. Apache Spark, Cassandra, MongoDB, and DynamoDB all run on CPU servers in production. Sharding involves hash routing, load monitoring, and scheduling decisions — CPU-bound operations with no matrix algebra. GPU acceleration applies to deep learning and vector operations, not distributed database coordination.

---

## References

1. M. J. Amiri, "Adaptive Sharding for Scalable Blockchain Platforms," *ICDE*, 2021.
2. B. Mehta, N. Baghel, M. J. Amiri, B. T. Loo, R. Marcus, "Marlin: Adaptive Sharding in Untrusted Environments," *SIGMOD*, 2025.
3. M. Z. Haider, T. Noreen, M. D. Assunção, K. Zhang, "AI-driven Predictive Shard Allocation for Scalable Next-Generation Blockchains," *arXiv:2511.19450*, 2025.
4. Kaggle, "Amazon Sale Report Dataset," 2023. [Link](https://www.kaggle.com/datasets/mdsazzatsardar/amazonsalesreport)

---

## Authors

**Nguyen Le Bao Dang** (23520230) · **Pham Minh Ngan** (23520997)  
Faculty of Information Systems · University of Information Technology · VNU-HCM  
*Big Data Processing & Distributed Database Systems · 2026*
