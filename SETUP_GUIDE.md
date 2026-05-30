# Hướng dẫn setup GitHub repo — từng bước

## Bước 1: Tạo repo trên GitHub

1. Vào https://github.com → click **New repository**
2. Đặt tên: `LPR-Ecommerce-Sharding`
3. Description: `Lightweight Proactive Re-sharding for trusted distributed databases in E-commerce`
4. Public ✓
5. **Không** tick "Add a README" (mình đã có rồi)
6. Click **Create repository**

---

## Bước 2: Chuẩn bị local repo

```bash
# Clone cấu trúc này về (hoặc dùng thư mục đã có)
cd LPR_project

# Khởi tạo git
git init
git branch -M main

# Add remote (thay YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/LPR-Ecommerce-Sharding.git
```

---

## Bước 3: Cấu trúc thư mục cần có trước khi commit

```
LPR-Ecommerce-Sharding/
├── README.md                          ✅ đã có
├── requirements.txt                   ✅ đã có
├── .gitignore                         ✅ đã có
├── Proactive_ReSharding.ipynb         ← copy từ file gốc vào đây
├── lpr/
│   ├── __init__.py                    ✅
│   ├── shard.py                       ✅
│   ├── lpr_sharding.py               ✅
│   └── metrics.py                    ✅
├── baselines/
│   ├── __init__.py                    ✅
│   ├── static_sharding.py            ✅
│   └── reactive_sharding.py          ✅
├── experiments/
│   ├── __init__.py                    ✅
│   ├── workload_generator.py         ✅
│   └── simulation.py                 ✅
├── visualization/
│   ├── __init__.py                    ✅
│   └── plot_results.py               ✅
├── docs/
│   └── assets/
│       └── architecture.png          ← screenshot diagram từ chat này
└── paper/
    └── LPR_paper.pdf                 ← copy từ file PDF
```

---

## Bước 4: Lấy screenshot architecture diagram

1. Scroll lên phần diagram trong chat này
2. Right-click → Save image as → `architecture.png`
3. Đặt vào `docs/assets/architecture.png`

---

## Bước 5: Cập nhật màu sắc trong notebook

Tìm tất cả dòng có `color=['yellow', 'red', 'lightgreen']` trong notebook và thay bằng:

```python
# TRƯỚC (xấu):
color=['yellow', 'red', 'lightgreen']

# SAU (đẹp):
STRATEGY_COLORS = ['#6B7280', '#EF6C5E', '#3B82C4']
color=STRATEGY_COLORS
```

Thêm cell này vào đầu Section 4 (phần visualization):

```python
# ── Color palette chuẩn cho toàn bộ biểu đồ ──────────────────────
STRATEGY_COLORS = {
    'Static':   '#6B7280',   # slate gray
    'Reactive': '#EF6C5E',   # soft coral
    'LPR':      '#3B82C4',   # cool blue
}
COLORS_LIST = list(STRATEGY_COLORS.values())

import matplotlib.pyplot as plt
plt.rcParams.update({
    'font.family':     'DejaVu Sans',
    'axes.spines.top':  False,
    'axes.spines.right': False,
    'axes.grid':        True,
    'grid.alpha':       0.35,
    'grid.linestyle':   '--',
    'figure.dpi':       130,
})
```

---

## Bước 6: Commit và push

```bash
git add .
git commit -m "feat: initial LPR project — algorithm, baselines, experiments, visualization

- LPR core: EMA filter + Growth Rate + Adaptive Threshold + Cost-Benefit gate
- Baselines: StaticSharding, ReactiveSharding
- Experiments: Workload generator (Uniform/Zipfian/Flash-sale), simulation runner
- Visualization: Professional palette replacing yellow/red/green
- Paper: IEEE-format research paper
- Results: 87% migration reduction, 30.8% throughput gain on Zipfian workload"

git push -u origin main
```

---

## Bước 7: Thêm GitHub Topics

Vào repo → About (gear icon) → Topics:
```
distributed-systems  database-sharding  load-balancing  
big-data  pyspark  ecommerce  python  research
```

---

## Bước 8: Enable GitHub Pages (optional — cho docs)

Settings → Pages → Source: `main` branch, `/docs` folder → Save

URL sẽ là: `https://YOUR_USERNAME.github.io/LPR-Ecommerce-Sharding/`

---

## Checklist trước khi gửi link cho nhà tuyển dụng

- [ ] README render đẹp trên GitHub (kiểm tra preview)
- [ ] Notebook chạy được từ đầu đến cuối không lỗi
- [ ] Architecture diagram hiển thị trong README
- [ ] Tất cả màu sắc chart đã được đổi sang palette mới
- [ ] Paper PDF có trong thư mục `paper/`
- [ ] GitHub Topics đã được thêm
- [ ] Description repo đã có stats: "87% migration reduction · 30.8% throughput"
