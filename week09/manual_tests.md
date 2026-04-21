Inventory automated test
python -m pytest -q tests/test_inventory.py --application inventory

checks to see:
- reserve reduces availability
- over-allocation is blocked
- release restores availability
- invalid release does not corrupt state


Full selected-application test
python -m pytest -q --application inventory

checks to see:
the project behaves correctly for the selected application


End to end testing:
python scripts/run_cluster.py

python apps/inventory_client.py create-item test-item-1 5
python apps/inventory_client.py reserve test-item-1 r1 2
python apps/inventory_client.py get test-item-1
python apps/inventory_client.py release test-item-1 r1
python apps/inventory_client.py get test-item-1

checks:
create, reserve, release, and get all work correctly
the same item is routed consistently

Over-allocation failure test
python apps/inventory_client.py create-item test-item-2 1
python apps/inventory_client.py reserve test-item-2 r1 1
python apps/inventory_client.py reserve test-item-2 r2 1
python apps/inventory_client.py get test-item-2

checks: 
over-allocation is prevented
failed transactions do not corrupt state

Invalid release failure test
python apps/inventory_client.py create-item test-item-3 2
python apps/inventory_client.py reserve test-item-3 r1 1
python apps/inventory_client.py release test-item-3 r2
python apps/inventory_client.py get test-item-3

checks:
invalid release requests are rejected
failed operations do not corrupt state

Routing consistency test
python apps/inventory_client.py create-item item-Z 5
python apps/inventory_client.py get item-Z

checks:
request routing is stable and deterministic

Storage persistence test
python apps/inventory_client.py create-item test-item-4 3
python apps/inventory_client.py reserve test-item-4 r1 1
python scripts/stop_cluster.py
python scripts/run_cluster.py
python apps/inventory_client.py get test-item-4
python scripts/stop_cluster.py

checks:
committed state survives restart
persistent storage is working correctly

Storage isolation check through failure cases
python scripts/run_cluster.py
python apps/inventory_client.py create-item test-item-5 1
python apps/inventory_client.py reserve test-item-5 r1 1
python apps/inventory_client.py reserve test-item-5 r2 1
python apps/inventory_client.py get test-item-5
python scripts/stop_cluster.py

checks:
failed writes do not partially modify stored state
storage remains consistent after rejected operations

Storage recovery after invalid release test
python scripts/run_cluster.py
python apps/inventory_client.py create-item test-item-6 2
python apps/inventory_client.py reserve test-item-6 r1 1
python apps/inventory_client.py release test-item-6 r2
python apps/inventory_client.py get test-item-6
python scripts/stop_cluster.py
python scripts/run_cluster.py
python apps/inventory_client.py get test-item-6
python scripts/stop_cluster.py

checks:
invalid operations do not damage persistent state
recovery still returns correct committed data after restart

Stop cluster
python scripts/stop_cluster.py