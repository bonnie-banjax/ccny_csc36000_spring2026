from __future__ import annotations
import math
from typing import List

import primes_pb2 # NOTE this isn't necessary for the sieve proper

# Sieve of Eratosthenes
def primes_sieve(low: int, high: int) -> bytearray: # BEGIN
  """
  Pure compute engine: Returns a bytearray where 1 is prime, 0 is composite.
  Assumes low >= 2 and high > low. Now always returns bytearray
  """
  # 1) Base primes up to sqrt(high-1)
  limit = int(math.isqrt(high - 1))
  base = bytearray(b"\x01") * (limit + 1)
  base[0:2] = b"\x00\x00"
  for p in range(2, int(limit**0.5) + 1):
    if base[p]:
      base[p*p:limit+1:p] = b"\x00" * (((limit - p*p)//p) + 1)
  base_primes = [i for i in range(limit + 1) if base[i]]

  # 2) Segmented sieve for [low, high)
  size = high - low
  seg = bytearray(b"\x01") * size

  for p in base_primes:
    pp = p * p
    if pp >= high:
      break
    start = (low + p - 1) // p * p
    if start < pp:
      start = pp
    # Not sure if this is actually faster, need to benchmark
    seg[start-low : high-low : p] = b"\x00" * (((high - 1 - start) // p) + 1)

  return seg
# END Sieve of Eratosthenes


# BEGIN "last mile" processing - caller knows "low" because they gave it
def worker_task(low: int, high: int, mode: int) -> primes_pb2.PerNodeResult:

  # caller responsible for valid (low, high) range -- saves cycles
  seg = primes_sieve(low, high)   # 1. Run the heavy compute

  # 2. Package into a gRPC message (The 'Last Mile')
  # this is here to show how to use the sieve
  res = primes_pb2.PerNodeResult()
  res.slice.extend([low, high]) # Use the 'low' we already have

  match mode:
    case primes_pb2.LIST:            # old return path 1 (list of primes)
      primes = [low + i for i, is_p in enumerate(seg) if is_p]
      res.primes.extend(primes)
      res.total_primes = len(primes)
    case primes_pb2.COUNT:           # old return path 2 (count aka sum)
      res.total_primes = sum(seg)
    case _:
      raise ValueError("Unknown execution mode")

  return res
# END "last mile" processing
