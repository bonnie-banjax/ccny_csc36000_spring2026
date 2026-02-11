PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

PROTO := proto/primes.proto
GEN_DIR := generated

.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  make install      create venv, install deps, generate protobuf stubs"
	@echo "  make gen          regenerate protobuf stubs"
	@echo "  make run-primary  start coordinator on 127.0.0.1:50051"
	@echo "  make run-worker   start worker on 127.0.0.1:50061 and register to coordinator"
	@echo "  make cli-count    sample distributed count run"
	@echo "  make cli-list     sample distributed list run"
	@echo "  make test         run pytest"
	@echo "  make clean        remove artifacts"

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)

venv: $(VENV)/bin/activate

deps: venv
	$(PIP) install -r requirements.txt

gen: deps
	mkdir -p $(GEN_DIR)
	$(PY) -m grpc_tools.protoc \
		-I proto \
		--python_out=$(GEN_DIR) \
		--grpc_python_out=$(GEN_DIR) \
		$(PROTO)

install: gen

run-primary: install
	$(PY) primary_node.py --host 127.0.0.1 --port 50051 --ttl 120

run-worker: install
	$(PY) secondary_node.py --host 127.0.0.1 --port 50061 --node-id worker-1 --primary 127.0.0.1:50051 --register-interval 10

cli-count: install
	$(PY) primes_cli.py --low 2 --high 100000 --exec distributed --mode count --primary 127.0.0.1:50051 --secondary-exec processes --secondary-workers 2 --time

cli-list: install
	$(PY) primes_cli.py --low 2 --high 500 --exec distributed --mode list --primary 127.0.0.1:50051 --secondary-exec threads --secondary-workers 4 --max-return-primes 200 --include-per-node --time

test: install
	$(PY) -m pytest -q

clean:
	rm -rf .venv .pytest_cache __pycache__ */__pycache__
	rm -f generated/primes_pb2.py generated/primes_pb2_grpc.py
