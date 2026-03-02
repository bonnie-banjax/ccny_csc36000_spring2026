These tests are black-box.

They assume you provide:
- scripts/run_cluster.sh
- scripts/stop_cluster.sh
- scripts/start_replica.sh <id>
- scripts/stop_replica.sh <id>
and that you implement the gRPC APIs in protos/.

Run:
  pip install -r requirements-test.txt
  pytest -q
