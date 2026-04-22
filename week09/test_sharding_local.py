#RUN: python test_sharding_local.py
#Checks to see the same item always maps to the same logical shard
'''

from student_impl.sharding import build_partition_key, choose_logical_shard

payload = {
    "item_id": "item-1",
    "reservation_id": "res-1",
    "quantity": 2
}

key = build_partition_key("inventory", "reserve", payload)

print("Partition key:", key)
print("Shard:", choose_logical_shard(key))
print("Shard again:", choose_logical_shard(key))
print("Shard again:", choose_logical_shard(key))

'''

# Sharding distributation test
# RUN: python test_sharding_local.py
# Checks to hash-based sharding is reasonably balanced
from collections import Counter
from student_impl.sharding import choose_logical_shard

counts = Counter()

for i in range(200):
    key = f"item-{i}"
    shard = choose_logical_shard(key)
    counts[shard] += 1

print("Shard distribution:")
for shard, count in sorted(counts.items()):
    print(f"Shard {shard}: {count}")