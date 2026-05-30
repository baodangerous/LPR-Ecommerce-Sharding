"""
lpr/shard.py
------------
Đơn vị lưu trữ logic trong cụm phân tán.
Mỗi Shard theo dõi tải hiện tại và tần suất truy cập theo từng key.
"""
from collections import defaultdict


class Shard:
    """Logical storage node — tracks load and per-key request frequency."""

    def __init__(self, shard_id: int):
        self.shard_id = shard_id
        self.current_load: int = 0
        self.request_counter: dict = defaultdict(int)
        self.stored_keys: set = set()
        self.migration_count: int = 0

    def process_request(self, key) -> None:
        self.request_counter[key] += 1
        self.current_load += 1
        self.stored_keys.add(key)

    def reset_load(self) -> None:
        """Reset load counters at end of each time window."""
        self.current_load = 0
        self.request_counter = defaultdict(int)

    def __repr__(self) -> str:
        return f"Shard(id={self.shard_id}, load={self.current_load}, keys={len(self.stored_keys)})"
