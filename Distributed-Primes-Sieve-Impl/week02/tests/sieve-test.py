
# python3 -m grpc_tools.protoc -I week02/infra --python_out=week02/core --grpc_python_out=week02/core primes.proto

import primes_pb2
from primes_in_range import primes_sieve, worker_task

# BEGIN first round of generated
# --- The "Mock" Constants (from your types.py or alias block) ---
class PRIMES_MODE:
    COUNT = primes_pb2.Mode.COUNT
    LIST = primes_pb2.Mode.LIST

# --- The Test Script ---


def sieve_test_only():
  # Test Case 1: Validate the "Pure Engine" (prime_sieve)
  # Range [10, 20) -> Primes are 11, 13, 17, 19
  low, high = 10, 20
  print(f"Testing sieve on range [{low}, {high})...")
  seg = primes_sieve(low, high)

  assert len(seg) == (high - low), "Sieve size mismatch!"
  assert sum(seg) == 4, f"Expected 4 primes, found {sum(seg)}"
  print("prime_sieve: Logic Validated.")

def run_test():
    print("--- Starting Validation Test ---")

    # Test Case 1: Validate the "Pure Engine" (prime_sieve)
    # Range [10, 20) -> Primes are 11, 13, 17, 19
    low, high = 10, 20
    print(f"Testing sieve on range [{low}, {high})...")
    seg = primes_sieve(low, high)

    assert len(seg) == (high - low), "Sieve size mismatch!"
    assert sum(seg) == 4, f"Expected 4 primes, found {sum(seg)}"
    print("prime_sieve: Logic Validated.")

    # Test Case 2: Validate "worker_task" (Protobuf Schema integration)
    print("Testing worker_task with Mode.LIST...")
    res = worker_task(low, high, PRIMES_MODE.LIST)

    # Validate the result is a Protobuf object
    assert isinstance(res, primes_pb2.PerNodeResult), "Output is not a Protobuf message!"

    # Validate the data inside the Protobuf object
    assert res.total_primes == 4, "Protobuf total_primes mismatch"
    assert list(res.primes) == [11, 13, 17, 19], f"Protobuf primes list incorrect: {list(res.primes)}"
    assert list(res.slice) == [10, 20], "Protobuf slice bounds incorrect"

    print("worker_task (LIST): Protobuf Schema Validated.")

    # Test Case 3: Validate Mode.COUNT behavior
    print("Testing worker_task with Mode.COUNT...")
    res_count = worker_task(low, high, PRIMES_MODE.COUNT)

    assert res_count.total_primes == 4
    assert len(res_count.primes) == 0, "Primes list should be empty in COUNT mode"

    print("worker_task (COUNT): Logic Fork Validated.")

    print("\n--- ALL TESTS PASSED ---")
    print("The code is ready for the secondary_node executor.")
# END

# BEGIN

import logging
from google.protobuf import text_format
import primes_pb2

# Configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    filename='sieve_validation.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

def defunct_run_logged_test():
    print("--- Starting Validation Test (See sieve_validation.log for details) ---")
    logger.info("=== SIEVE VALIDATION LOG ===")

    # 1. Representative Range Setup
    # Primes between 2000 and 3000: there are exactly 135.
    low, high = 2000, 3000
    expected_count = 135

    # 2. Validate Pure Engine Artifact
    logger.info(f"Step 1: Testing prime_sieve on range [{low}, {high})")
    seg = primes_sieve(low, high)

    actual_sum = sum(seg)

    logger.info(f"Engine Output: bytearray length {len(seg)}")
    logger.info(f"Engine Density: {sum(seg)} bits set to 1")
# BEGIN
    logger.info(f"Actual: {actual_sum}, Expected: {expected_count}")

    # Check logic with a diagnostic message
    if actual_sum != expected_count:
      logger.error(f"FAIL: Engine sum mismatch. Actual: {actual_sum}, Expected: {expected_count}")
      # Log the first 50 bytes of the sieve to see the pattern of 1s and 0s
      logger.error(f"Sieve snippet: {list(seg[:50])}")
      raise AssertionError(f"Sieve logic error: {actual_sum} != {expected_count}")
# END



    assert len(seg) == (high - low)
    assert sum(seg) == expected_count
    print("prime_sieve: Logic Validated.")

    # 3. Validate worker_task (LIST Mode)
    logger.info(f"\nStep 2: Testing worker_task with Mode.LIST")
    res_list = worker_task(low, high, primes_pb2.LIST)

# BEGIN
    try:
        # We wrap the assertions in a try block so we can log the object on failure
        assert res.total_primes == expected_count
        assert len(res.primes) == expected_count
    except AssertionError as e:
        # This is the "Inspectable Intermediate Artifact" you need
        logger.error("!!! TEST FAILED: Inspecting Protobuf Artifact !!!")
        logger.error(text_format.MessageToString(res))
        print(f"Test failed. Check the log for the full Protobuf dump.")
        raise e # Re-throw so the test still "fails" officially
# END

    # Write the actual Protobuf message structure to the log
    # text_format produces a clean, inspectable YAML-like artifact
    logger.info("Protobuf Artifact (Mode.LIST):")
    logger.info(text_format.MessageToString(res_list, as_utf8=True))

    assert res_list.total_primes == expected_count
    assert len(res_list.primes) == expected_count
    assert res_list.primes[0] == 2003  # First prime in range
    assert res_list.primes[-1] == 2999 # Last prime in range
    print("worker_task (LIST): Protobuf Schema Validated.")

    # 4. Validate worker_task (COUNT Mode)
    logger.info(f"\nStep 3: Testing worker_task with Mode.COUNT")
    res_count = worker_task(low, high, primes_pb2.COUNT)

    logger.info("Protobuf Artifact (Mode.COUNT):")
    logger.info(text_format.MessageToString(res_count, as_utf8=True))

    assert res_count.total_primes == expected_count
    assert len(res_count.primes) == 0
    print("worker_task (COUNT): Logic Fork Validated.")

    print("\n--- ALL TESTS PASSED ---")
    logger.info("\n=== VALIDATION SUCCESSFUL ===")
# END

# BEGIN
def run_logged_test(low = 2, high = 29):
    print("--- Starting Validation Test (Non-Aborting, check sieve_validation.log) ---")
    logger.info("=== SIEVE VALIDATION LOG ===")

    # 1. Representative Range Setup
    low, high = 2000, 3000
    expected_count = 127 # lmao not 135

    # 2. Validate Pure Engine Artifact
    logger.info(f"Step 1: Testing prime_sieve on range [{low}, {high})")
    seg = primes_sieve(low, high)

    seg_len = len(seg)
    seg_sum = sum(seg)

    logger.info(f"{seg_len == (high - low)} | len(seg) == {seg_len} (Expected: {high - low})")
    logger.info(f"{seg_sum == expected_count} | sum(seg) == {seg_sum} (Expected: {expected_count})")

    # 3. Validate worker_task (LIST Mode)
    logger.info(f"\nStep 2: Testing worker_task with Mode.LIST")
    res_list = worker_task(low, high, primes_pb2.LIST)

    # Always log artifact for inspection
    logger.info("Protobuf Artifact (Mode.LIST):")
    logger.info(text_format.MessageToString(res_list, as_utf8=True))

    logger.info(f"{res_list.total_primes == expected_count} | res_list.total_primes == {res_list.total_primes} (Expected: {expected_count})")
    logger.info(f"{len(res_list.primes) == expected_count} | len(res_list.primes) == {len(res_list.primes)} (Expected: {expected_count})")

    # Edge case value checks
    first_p = res_list.primes[0] if len(res_list.primes) > 0 else "N/A"
    last_p = res_list.primes[-1] if len(res_list.primes) > 0 else "N/A"

    logger.info(f"{first_p == 2003} | res_list.primes[0] == {first_p} (Expected: 2003)")
    logger.info(f"{last_p == 2999} | res_list.primes[-1] == {last_p} (Expected: 2999)")

    # 4. Validate worker_task (COUNT Mode)
    logger.info(f"\nStep 3: Testing worker_task with Mode.COUNT")
    res_count = worker_task(low, high, primes_pb2.COUNT)

    logger.info("Protobuf Artifact (Mode.COUNT):")
    logger.info(text_format.MessageToString(res_count, as_utf8=True))

    logger.info(f"{res_count.total_primes == expected_count} | res_count.total_primes == {res_count.total_primes} (Expected: {expected_count})")
    logger.info(f"{len(res_count.primes) == 0} | len(res_count.primes) == {len(res_count.primes)} (Expected: 0)")

    print("--- Test Run Complete (Check log for boolean failures) ---")
    logger.info("\n=== END OF VALIDATION LOG ===")
#  END

if __name__ == "__main__":
    # Note: Import your functions here or ensure they are in the namespace
    # from primes_in_range import primes_sieve, worker_task
    run_logged_test()
    # sieve_test_only()

#####################################

