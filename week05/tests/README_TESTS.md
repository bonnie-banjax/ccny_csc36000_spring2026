These tests are black-box.

They assume you provide:
- scripts/run_cluster.py
- scripts/stop_cluster.py
- scripts/start_replica.py <id>
- scripts/stop_replica.py <id>
and that you implement the gRPC APIs in protos/.

Run:
  pip install -r requirements.txt
  pytest -q
