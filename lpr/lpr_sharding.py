"""
lpr/lpr_sharding.py
-------------------
Cơ chế Lightweight Proactive Re-sharding (LPR).

Pipeline mỗi chu kỳ t:
  1. Thu thập Lᵢ(t)
  2. Cập nhật EMAᵢ(t) và GRᵢ(t)
  3. Tính Tₐ(t)
  4. Tính Hᵢ(t)
  5. Nếu Hᵢ > Tₐ → Cost-Benefit check
  6. Nếu profitable → migrate top-K hot keys sang coolest shard
  7. Cập nhật key_to_shard_map
"""
import logging
import numpy as np
from typing import List

from .shard import Shard
from .metrics import (
    compute_growth_rate,
    compute_ema,
    compute_adaptive_threshold,
    compute_hotspot_score,
)

logger = logging.getLogger(__name__)


class LPRSharding:
    """
    Lightweight Proactive Re-sharding.

    Parameters
    ----------
    num_shards : int
    alpha : float        EMA smoothing factor (default 0.3)
    beta : float         Growth Rate weight in hotspot score (default 0.5)
    gamma : float        Adaptive threshold safety margin (default 1.5)
    migration_fraction : float   Fraction of hot keys migrated per trigger (default 0.1)
    cost_barrier : float         Cost-benefit barrier K — migrate only if benefit > K*cost (default 1.2)
    """

    def __init__(
        self,
        num_shards: int,
        alpha: float = 0.3,
        beta: float = 0.5,
        gamma: float = 1.5,
        migration_fraction: float = 0.1,
        cost_barrier: float = 1.2,
    ):
        self.num_shards = num_shards
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.migration_fraction = migration_fraction
        self.cost_barrier = cost_barrier

        self.ema_loads: List[float] = [0.0] * num_shards
        self.previous_loads: List[float] = [0.0] * num_shards
        self.key_to_shard_map: dict = {}
        self.total_migrations: int = 0

    def get_shard_for_key(self, key) -> int:
        if key in self.key_to_shard_map:
            return self.key_to_shard_map[key]
        shard_id = hash(key) % self.num_shards
        self.key_to_shard_map[key] = shard_id
        return shard_id

    def rebalance(self, shards: List[Shard]) -> None:
        current_loads = [s.current_load for s in shards]
        total = sum(current_loads)
        if total == 0:
            self.previous_loads = current_loads[:]
            return

        # ── Step 1-2: Update EMA & Growth Rate ──────────────────────
        growth_rates = []
        for i in range(self.num_shards):
            self.ema_loads[i] = compute_ema(current_loads[i], self.ema_loads[i], self.alpha)
            growth_rates.append(compute_growth_rate(current_loads[i], self.previous_loads[i]))
        self.previous_loads = current_loads[:]

        # ── Step 3-4: Adaptive Threshold & Hotspot Score ─────────────
        threshold = compute_adaptive_threshold(current_loads, self.gamma)
        hotspot_scores = [
            compute_hotspot_score(self.ema_loads[i], growth_rates[i], self.beta)
            for i in range(self.num_shards)
        ]

        # ── Step 5: Find hottest candidate above threshold ───────────
        hot_id = -1
        max_score = -float("inf")
        for i, score in enumerate(hotspot_scores):
            if score > threshold and score > max_score:
                max_score = score
                hot_id = i

        if hot_id == -1:
            return

        hot_shard = shards[hot_id]
        logger.debug(
            "LPR | hot shard=%d score=%.2f threshold=%.2f",
            hot_id, max_score, threshold,
        )

        active_keys = {k: v for k, v in hot_shard.request_counter.items() if v > 0}
        if not active_keys:
            return

        # ── Step 5: Cost-Benefit check ───────────────────────────────
        sorted_keys = sorted(active_keys.items(), key=lambda x: x[1], reverse=True)
        n_migrate = max(1, int(len(sorted_keys) * self.migration_fraction))
        keys_to_migrate = [k for k, _ in sorted_keys[:n_migrate]]

        expected_benefit = sum(active_keys[k] for k in keys_to_migrate)
        migration_cost = len(keys_to_migrate)   # 1 unit per key

        if expected_benefit <= self.cost_barrier * migration_cost:
            logger.debug(
                "LPR | cost-benefit gate blocked: benefit=%.1f cost=%.1f barrier=%.1f",
                expected_benefit, migration_cost, self.cost_barrier,
            )
            return

        # ── Step 6: Migrate to coolest shard ────────────────────────
        temp_loads = list(current_loads)
        temp_loads[hot_id] = float("inf")
        cool_id = int(np.argmin(temp_loads))

        if cool_id == hot_id:
            return

        target_shard = shards[cool_id]
        for key in keys_to_migrate:
            if key not in hot_shard.stored_keys:
                continue
            hot_shard.stored_keys.discard(key)
            hot_shard.request_counter.pop(key, None)
            target_shard.stored_keys.add(key)
            self.key_to_shard_map[key] = cool_id
            self.total_migrations += 1

        logger.info(
            "LPR | migrated %d keys from S%d → S%d",
            len(keys_to_migrate), hot_id, cool_id,
        )
