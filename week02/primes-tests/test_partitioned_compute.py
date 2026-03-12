from grpc_common import compute_partitioned
from primes_in_range import get_primes


def test_compute_partitioned_count_matches_direct() -> None:
    low, high = 2, 20000
    direct = int(get_primes(low, high, return_list=False))
    resp = compute_partitioned(low, high, mode="count", chunk=3000, exec_mode="threads", workers=4)
    assert resp["ok"] is True
    assert int(resp["total_primes"]) == direct


def test_compute_partitioned_list_prefix_and_truncation() -> None:
    low, high = 2, 500
    full = list(get_primes(low, high, return_list=True))
    cap = 20
    resp = compute_partitioned(low, high, mode="list", chunk=100, exec_mode="single", max_return_primes=cap)
    assert resp["ok"] is True
    assert resp["primes"] == full[:cap]
    assert resp["primes_truncated"] == (len(full) > cap)
    assert int(resp["total_primes"]) == len(full)
