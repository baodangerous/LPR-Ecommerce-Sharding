"""
experiments/workload_generator.py
----------------------------------
Sinh ba loại workload theo phân phối khác nhau.

Uniform  : baseline không skew
Zipfian  : phân phối long-tail thực tế (s=2.0)
Flash-sale: burst traffic tập trung vào một shard
"""
import numpy as np
from typing import Optional


def generate_uniform_workload(num_requests: int, num_products: int) -> np.ndarray:
    """Mỗi sản phẩm có xác suất truy cập như nhau — P(k) = 1/N."""
    return np.random.randint(0, num_products, size=num_requests)


def generate_zipfian_workload(
    num_requests: int,
    num_products: int,
    skew: float = 2.0,
) -> np.ndarray:
    """
    Phân phối Zipf — mô phỏng hành vi mua sắm thực tế.
    P(k) ∝ 1/kˢ  →  top 20% sản phẩm nhận ~80% request khi s=2.0.
    """
    samples = np.random.zipf(skew, size=num_requests)
    return samples % num_products


def generate_flash_sale_workload(
    num_requests: int,
    num_products: int,
    hot_product: int = 1,
    burst_ratio: float = 0.4,
) -> np.ndarray:
    """
    Burst traffic — 40% request đổ vào một sản phẩm duy nhất.
    Mô phỏng kịch bản Flash Sale / Livestream event.

    Parameters
    ----------
    burst_ratio : float  Tỷ lệ request tập trung vào hot_product (default 0.4 = 40%)
    """
    workload = np.random.randint(0, num_products, size=num_requests)
    burst_size = int(num_requests * burst_ratio)
    workload[:burst_size] = hot_product
    np.random.shuffle(workload)
    return workload
