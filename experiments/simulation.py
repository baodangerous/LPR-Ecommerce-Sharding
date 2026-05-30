"""
experiments/simulation.py
--------------------------
Hàm run_simulation() — pipeline đánh giá cho mọi chiến lược.
Trả về dict metrics chuẩn để so sánh.
"""
import time
import logging
import numpy as np
from collections import defaultdict
from typing import List, Optional, Dict, Any

from lpr.shard import Shard
from lpr.metrics import compute_imbalance_ratio

logger = logging.getLogger(__name__)


def run_simulation(
    workload_requests: np.ndarray,
    sharding_strategy,
    shards: List[Shard],
    time_steps: int = 100,
    all_product_skus: Optional[List] = None,
) -> Dict[str, Any]:
    """
    Chạy simulation trên một workload và strategy cụ thể.

    Returns
    -------
    dict with keys:
        total_throughput        float   req/s
        average_latency         float   seconds/req
        final_imbalance         float   Imbalance Ratio tại bước cuối
        all_imbalances          list    IR theo từng time step
        total_migrations        int     Tổng số lần di trú
        total_simulation_time   float   Tổng thời gian (giây)
    """
    if all_product_skus is None:
        n = int(workload_requests.max()) + 1 if len(workload_requests) > 0 else 100
        all_product_skus = [f"sku_{i}" for i in range(n)]

    num_shards = len(shards)
    all_imbalances: List[float] = []
    requests_per_step = max(1, len(workload_requests) // time_steps)

    logger.info(
        "Simulation start | strategy=%s requests=%d steps=%d",
        type(sharding_strategy).__name__, len(workload_requests), time_steps,
    )

    start_time = time.time()

    for step in range(time_steps):
        start_idx = step * requests_per_step
        end_idx = min(start_idx + requests_per_step, len(workload_requests))
        chunk_indices = workload_requests[start_idx:end_idx]

        # Route and process each request
        for idx in chunk_indices:
            sku = all_product_skus[int(idx) % len(all_product_skus)]
            if hasattr(sharding_strategy, "get_shard_for_key"):
                shard_id = sharding_strategy.get_shard_for_key(sku)
            else:
                shard_id = hash(sku) % num_shards
            shards[shard_id].process_request(sku)

        # Collect metrics
        loads = [s.current_load for s in shards]
        ir = compute_imbalance_ratio(loads) if sum(loads) > 0 else 1.0
        all_imbalances.append(ir)

        # Rebalance decision
        if hasattr(sharding_strategy, "rebalance"):
            sharding_strategy.rebalance(shards)

        # Reset per-cycle load
        for s in shards:
            s.reset_load()

    total_time = time.time() - start_time
    n_req = len(workload_requests)

    return {
        "total_throughput": n_req / total_time if total_time > 0 else 0,
        "average_latency": total_time / n_req if n_req > 0 else 0,
        "final_imbalance": all_imbalances[-1] if all_imbalances else 1.0,
        "all_imbalances": all_imbalances,
        "total_migrations": getattr(sharding_strategy, "total_migrations", 0),
        "total_simulation_time": total_time,
    }
