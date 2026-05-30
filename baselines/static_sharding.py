"""
baselines/static_sharding.py
baselines/reactive_sharding.py
-------------------------------
Hai baseline để so sánh với LPR.
"""

# ── static_sharding.py ────────────────────────────────────────────────────────

class StaticSharding:
    """
    Hash-based static sharding — không có migration, O(1) routing.
    Dùng làm baseline ceiling cho throughput (zero overhead).
    """

    def __init__(self, num_shards: int):
        self.num_shards = num_shards
        self.total_migrations: int = 0
        self.key_to_shard_map: dict = {}

    def get_shard_for_key(self, key) -> int:
        if key not in self.key_to_shard_map:
            self.key_to_shard_map[key] = hash(key) % self.num_shards
        return self.key_to_shard_map[key]

    def rebalance(self, shards) -> None:
        pass  # No-op — static never rebalances
