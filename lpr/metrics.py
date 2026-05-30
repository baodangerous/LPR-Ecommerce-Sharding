"""
lpr/metrics.py
--------------
Các hàm toán học cốt lõi của cơ chế LPR.

Công thức tham chiếu (từ paper):
  GRᵢ(t)  = (Lᵢ(t) - Lᵢ(t-1)) / (Lᵢ(t-1) + ε)
  EMAᵢ(t) = α·Lᵢ(t) + (1-α)·EMAᵢ(t-1)
  Tₐ(t)   = mean(Lⱼ) · γ
  Hᵢ(t)   = EMAᵢ(t) + β·GRᵢ(t)
"""
import numpy as np
from typing import List


EPSILON = 1e-5   # tránh chia cho 0 khi shard nhàn rỗi


def compute_growth_rate(current_load: float, previous_load: float) -> float:
    """Tốc độ tăng trưởng tải — phát hiện xu hướng tăng sớm."""
    return (current_load - previous_load) / (previous_load + EPSILON)


def compute_ema(current_load: float, previous_ema: float, alpha: float = 0.3) -> float:
    """Exponential Moving Average — lọc nhiễu workload ngắn hạn."""
    return alpha * current_load + (1.0 - alpha) * previous_ema


def compute_adaptive_threshold(loads: List[float], gamma: float = 1.5) -> float:
    """
    Ngưỡng thích ứng động — tự co giãn theo trạng thái hệ thống.
    Tₐ = mean(L) · γ
    """
    return float(np.mean(loads)) * gamma


def compute_hotspot_score(ema: float, growth_rate: float, beta: float = 0.5) -> float:
    """
    Điểm Hotspot tích hợp — kết hợp tải hiện tại và gia tốc tải.
    Hᵢ = EMAᵢ + β·GRᵢ
    """
    return ema + beta * growth_rate


def compute_imbalance_ratio(loads: List[float]) -> float:
    """Chỉ số mất cân bằng tải toàn cụm."""
    arr = np.array(loads, dtype=float)
    mean = np.mean(arr)
    if mean == 0:
        return 1.0
    return float(np.max(arr) / mean)
