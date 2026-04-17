from __future__ import annotations

import pytest


def test_wallet_transfer_is_atomic(selected_application, gateway_stub):
    if selected_application != "wallet":
        pytest.skip("wallet tests only run when SELECTED_APPLICATION == 'wallet'")

    gateway_pb2, stub = gateway_stub
    stub.CreateAccount(gateway_pb2.CreateAccountRequest(account_id="a", initial_balance_cents=10000), timeout=5.0)
    stub.CreateAccount(gateway_pb2.CreateAccountRequest(account_id="b", initial_balance_cents=2500), timeout=5.0)
    transfer = stub.Transfer(
        gateway_pb2.TransferRequest(from_account_id="a", to_account_id="b", amount_cents=1250),
        timeout=5.0,
    )
    assert transfer.committed is True
    assert transfer.from_balance_cents == 8750
    assert transfer.to_balance_cents == 3750


def test_wallet_total_balance_preserved(selected_application, gateway_stub):
    if selected_application != "wallet":
        pytest.skip("wallet tests only run when SELECTED_APPLICATION == 'wallet'")

    gateway_pb2, stub = gateway_stub
    stub.CreateAccount(gateway_pb2.CreateAccountRequest(account_id="c", initial_balance_cents=20000), timeout=5.0)
    stub.CreateAccount(gateway_pb2.CreateAccountRequest(account_id="d", initial_balance_cents=0), timeout=5.0)
    stub.Transfer(
        gateway_pb2.TransferRequest(from_account_id="c", to_account_id="d", amount_cents=5000),
        timeout=5.0,
    )
    account_c = stub.GetAccount(gateway_pb2.GetAccountRequest(account_id="c"), timeout=5.0)
    account_d = stub.GetAccount(gateway_pb2.GetAccountRequest(account_id="d"), timeout=5.0)
    assert account_c.balance_cents + account_d.balance_cents == 20000


def test_wallet_unhappy_path_insufficient_funds_preserves_balances(selected_application, gateway_stub):
    if selected_application != "wallet":
        pytest.skip("wallet tests only run when SELECTED_APPLICATION == 'wallet'")

    gateway_pb2, stub = gateway_stub
    stub.CreateAccount(gateway_pb2.CreateAccountRequest(account_id="low", initial_balance_cents=500), timeout=5.0)
    stub.CreateAccount(gateway_pb2.CreateAccountRequest(account_id="dest", initial_balance_cents=700), timeout=5.0)

    with pytest.raises(Exception):
        stub.Transfer(
            gateway_pb2.TransferRequest(from_account_id="low", to_account_id="dest", amount_cents=600),
            timeout=5.0,
        )

    low = stub.GetAccount(gateway_pb2.GetAccountRequest(account_id="low"), timeout=5.0)
    dest = stub.GetAccount(gateway_pb2.GetAccountRequest(account_id="dest"), timeout=5.0)
    assert low.balance_cents == 500
    assert dest.balance_cents == 700
