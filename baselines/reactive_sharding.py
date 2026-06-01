"""
baselines/reactive_sharding.py
-------------------------------
Reactive Re-sharding — kích hoạt migration *sau khi* tải vượt ngưỡng cố định.
Đây là trạng thái nghệ thuật hiện tại mà LPR cải thiện.
"""
import logging
import numpy as np
from typing import List

from lpr.shard import Shard

logger = logging.getLogger(__name__)


class ReactiveSharding:
    """
    Reactive Re-sharding.

    Limitations vs LPR:
    - Phản ứng sau khi hotspot đã xảy ra (high latency spike)
    - Fixed threshold không thích ứng với workload dynamics
    - Gây migration thrashing dưới Zipfian/Flash-sale load
    """

    def __init__(
        self,
        num_shards: int,
        threshold: int = 150,
        migration_fraction: float = 0.1,
    ):
        self.num_shards = num_shards
        self.threshold = threshold
        self.migration_fraction = migration_fraction
        self.key_to_shard_map: dict = {}
        self.total_migrations: int = 0

    def get_shard_for_key(self, key) -> int:
        if key not in self.key_to_shard_map:
            self.key_to_shard_map[key] = hash(key) % self.num_shards
        return self.key_to_shard_map[key]

    def rebalance(self, shards: List[Shard]) -> None:
        current_loads = [s.current_load for s in shards]
        if not current_loads or sum(current_loads) == 0:
            return

        hot_id = int(np.argmax(current_loads))
        if current_loads[hot_id] <= self.threshold:
            return

        hot_shard = shards[hot_id]
        active_keys = {k: v for k, v in hot_shard.request_counter.items() if v > 0}
        if not active_keys:
            return

        sorted_keys = sorted(active_keys.items(), key=lambda x: x[1], reverse=True)
        n_migrate = max(1, int(len(sorted_keys) * self.migration_fraction))
        keys_to_migrate = [k for k, _ in sorted_keys[:n_migrate]]

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
            "Reactive | migrated %d keys from S%d → S%d",
            len(keys_to_migrate), hot_id, cool_id,
        )
