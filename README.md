# ⚡ LPR — Lightweight Proactive Re-sharding

<p align="center">
  <img src="docs/assets/architecture.png" alt="LPR Architecture" width="700"/>
</p>

<p align="center">
  <a href="#"><img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white"/></a>
  <a href="#"><img src="https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white"/></a>
  <a href="#"><img src="https://img.shields.io/badge/Platform-Google%20Colab-F9AB00?style=flat-square&logo=googlecolab&logoColor=white"/></a>
  <a href="#"><img src="https://img.shields.io/badge/Dataset-Amazon%20Sales-FF9900?style=flat-square&logo=amazon&logoColor=white"/></a>
  <a href="#"><img src="https://img.shields.io/badge/License-MIT-22C55E?style=flat-square"/></a>
</p>

<p align="center">
  <strong>Giảm 87% migration overhead · Tăng 30.8% throughput · Giảm 35.6% latency</strong><br/>
  <sub>So với Reactive Re-sharding trên tập dữ liệu Amazon Sale Report (128,975 giao dịch)</sub>
</p>

---

## 🔍 Vấn đề: Flash Sale làm tê liệt database phân tán

Trong hệ thống thương mại điện tử, một sự kiện Flash Sale có thể biến **top 20% sản phẩm thành 80% request** chỉ trong vài giây. Hiện tượng này — được gọi là **workload skew** theo phân phối Zipf — tạo ra các *hot-spot shard* làm tắc nghẽn toàn hệ thống.

```
Imbalance Ratio = max(Loadᵢ) / avg(Loadⱼ)

Trên tập dữ liệu thực: IR = 43.12  →  một shard gánh gấp 43 lần mức trung bình
```

**Các giải pháp hiện tại đều có điểm yếu:**

| Giải pháp | Vấn đề |
|---|---|
| Static Sharding | Không thích ứng, hot-spot cố định |
| Reactive Re-sharding | Phản ứng *sau khi* quá tải, gây migration thrashing |
| PSAP / Marlin (SOTA 2025) | Thiết kế cho môi trường blockchain không tin cậy → coordination overhead thặng dư |

---

## 💡 Insight: Phát hiện hotspot *trước khi* nó xảy ra

LPR không chờ tải vượt ngưỡng — nó theo dõi **tốc độ tăng trưởng tải** và đưa ra quyết định di trú *chủ động*.

```
# Hai tín hiệu cốt lõi:
GRᵢ(t)  = (Lᵢ(t) - Lᵢ(t-1)) / (Lᵢ(t-1) + ε)   # Gia tốc tải
EMAᵢ(t) = α·Lᵢ(t) + (1-α)·EMAᵢ(t-1)            # Lọc nhiễu (α=0.3)

# Hotspot score tích hợp:
Hᵢ(t) = EMAᵢ(t) + β·GRᵢ(t)    (β = 0.5)

# Chỉ migrate khi có lợi:
Expected Benefit > 1.2 × Migration Cost
```

---

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────┐
│              Trusted Distributed Environment        │
│                                                     │
│  Workload Generator                                 │
│  [Uniform] [Zipfian s=2.0] [Flash-sale burst=40%]  │
│       │                                             │
│       ▼                                             │
│  Request Router  ←──── key_to_shard_map ────────┐  │
│  (Consistent Hashing)                           │  │
│       │                                         │  │
│       ▼                                         │  │
│  ┌────┬────┬─────────┬────┐                     │  │
│  │ S₀ │ S₁ │ S₂ 🔥  │ S₃ │  M = 4 shards      │  │
│  └────┴────┴─────────┴────┘                     │  │
│       │   Lᵢ(t) per cycle                       │  │
│       ▼                                         │  │
│  ┌──────────── LPR Controller ──────────────┐   │  │
│  │ EMA filter → Growth Rate → Hotspot Score │   │  │
│  │ → Adaptive Threshold → Cost-Benefit gate ├───┘  │
│  └───────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────┘
```

---

## 📊 Kết quả thực nghiệm

### So sánh tổng hợp (50,000 requests, 4 shards)

| Kịch bản | Cơ chế | Throughput | Latency (µs) | Migrations | IR |
|---|---|---|---|---|---|
| **Uniform** | Static | 0.91M req/s | 1.10 | 0 | 1.20 |
| | Reactive | 0.57M req/s | 1.74 | 0 | 1.20 |
| | **LPR** | **0.89M req/s** | **1.12** | 12 | **1.10** |
| **Zipfian** | Static | 0.97M req/s | 1.03 | 0 | 2.90 |
| | Reactive | 0.68M req/s | 1.46 | 100 | 2.54 |
| | **LPR** | **0.89M req/s** | **1.12** | **13** | **2.54** |
| **Flash-sale** | Static | 1.08M req/s | 0.93 | 0 | 2.22 |
| | Reactive | 0.84M req/s | 1.19 | 200 | 2.25 |
| | **LPR** | **0.85M req/s** | **1.17** | **28** | **2.25** |

### Cải thiện của LPR so với Reactive

| Chỉ số | Uniform | Zipfian | Flash-sale |
|---|---|---|---|
| Throughput | **+56.1%** | **+30.8%** | +1.2% |
| Latency | **−35.6%** | **−23.3%** | −1.7% |
| Migrations | N/A | **−87%** | **−86%** |
| Monitoring cost | −35.6% | −23.3% | −1.7% |

---

## 📁 Cấu trúc project

```
LPR-Ecommerce-Sharding/
│
├── 📓 Proactive_ReSharding.ipynb   # Notebook chính (chạy trực tiếp trên Colab)
│
├── lpr/                            # Core algorithm
│   ├── __init__.py
│   ├── shard.py                    # Lớp Shard
│   ├── lpr_sharding.py             # LPRSharding — thuật toán chính
│   └── metrics.py                  # EMA, Growth Rate, Hotspot Score, Adaptive Threshold
│
├── baselines/                      # Baseline để so sánh
│   ├── __init__.py
│   ├── static_sharding.py          # StaticSharding
│   └── reactive_sharding.py        # ReactiveSharding
│
├── experiments/                    # Workload generation & simulation runner
│   ├── __init__.py
│   ├── workload_generator.py       # Uniform / Zipfian / Flash-sale
│   └── simulation.py               # run_simulation() pipeline
│
├── visualization/                  # Plotting utilities
│   └── plot_results.py             # Tất cả biểu đồ với palette chuẩn
│
├── data/
│   └── Amazon Sale Report.csv      # Dataset (tải từ Kaggle)
│
├── docs/
│   └── assets/
│       └── architecture.png        # Diagram kiến trúc
│
├── paper/
│   └── LPR_paper.pdf               # Bài báo IEEE format
│
└── requirements.txt
```

---

## 🚀 Cách chạy

### Option 1: Google Colab (khuyến nghị)

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/YOUR_USERNAME/LPR-Ecommerce-Sharding/blob/main/Proactive_ReSharding.ipynb)

1. Click badge trên
2. Upload `Amazon Sale Report.csv` vào `/content/drive/MyDrive/LPR_Project/datasets/`
3. Run All

### Option 2: Local

```bash
git clone https://github.com/YOUR_USERNAME/LPR-Ecommerce-Sharding.git
cd LPR-Ecommerce-Sharding
pip install -r requirements.txt

# Chạy simulation nhanh (không cần dataset thực)
python -c "
from experiments.workload_generator import generate_zipfian_workload
from experiments.simulation import run_simulation
from lpr.lpr_sharding import LPRSharding
from lpr.shard import Shard

workload = generate_zipfian_workload(50000, 100, skew=2.0)
shards = [Shard(i) for i in range(4)]
strategy = LPRSharding(num_shards=4)
result = run_simulation(workload, strategy, shards)
print(f'Throughput: {result[\"total_throughput\"]:,.0f} req/s')
print(f'Migrations: {result[\"total_migrations\"]}')
print(f'Imbalance:  {result[\"final_imbalance\"]:.2f}')
"
```

---

## ⚙️ Tham số chính

| Tham số | Giá trị | Ý nghĩa |
|---|---|---|
| `alpha` | 0.3 | Hệ số làm mịn EMA |
| `beta` | 0.5 | Trọng số Growth Rate trong Hotspot Score |
| `gamma` | 1.5 | Biên an toàn Adaptive Threshold |
| `migration_fraction` | 0.1 | % key di chuyển mỗi lần |
| `K` (cost-benefit) | 1.2 | Rào cản anti-thrashing |

---

## 🔬 So sánh với SOTA

| Tiêu chí | **LPR** | PSAP (2025) | Marlin (SIGMOD 2025) |
|---|---|---|---|
| Môi trường | Trusted (Enterprise) | Untrusted (Blockchain) | Untrusted (Blockchain) |
| Cơ chế dự báo | EMA + Growth Rate | Safe-PPO RL | Byzantine consensus |
| Độ phức tạp | **O(k)/cycle** | Cao | Cao |
| Huấn luyện mô hình | **Không cần** | Cần | Không cần |
| Tính xác định | **Hoàn toàn** | Cần validator sync | Partial |
| Phần cứng chuyên dụng | **Không cần** | Cần | Không cần |

> LPR không cạnh tranh với PSAP/Marlin trong không gian blockchain. LPR lấp đầy khoảng trống: **hệ CSDL thương mại điện tử tin cậy**, nơi không cần Byzantine overhead.

---

## 📚 Tài liệu tham khảo

```bibtex
@article{lpr2025,
  title   = {Cơ chế tái phân mảnh chủ động nhẹ cho hệ cơ sở dữ liệu phân tán
             trong môi trường tin cậy: Áp dụng trên dữ liệu thương mại điện tử},
  author  = {Nguyễn Lê Bảo Đăng and Phạm Minh Ngân},
  year    = {2025},
  school  = {Trường Đại học Công nghệ Thông tin – ĐHQG TPHCM}
}
```

---

## 👥 Tác giả

| | Nguyễn Lê Bảo Đăng | Phạm Minh Ngân |
|---|---|---|
| MSSV | 23520230 | 23520997 |
| Khoa | Hệ thống thông tin | Hệ thống thông tin |
| Trường | UIT – ĐHQG TPHCM | UIT – ĐHQG TPHCM |

---

<p align="center">
  <sub>Built for Big Data & Distributed Systems coursework · UIT 2025</sub>
</p>
