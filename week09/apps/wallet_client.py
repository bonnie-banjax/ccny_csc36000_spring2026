#!/usr/bin/env python3
from __future__ import annotations

"""
Provided wallet / transfer application client.

Application model:
- account_id: identifies one account
- initial_balance_cents: starting balance for a created account
- amount_cents: transfer amount, represented as integer cents

Provided operations:
- create account_id initial_balance_cents
- get account_id
- transfer from_account_id to_account_id amount_cents

What the application expects from the student implementation:
- Money must not be created or destroyed.
- A transfer must be atomic: debit and credit happen together or not at all.
- Reads must not observe a half-applied transfer.
- Failed transfers must leave both accounts unchanged.

What the tests do with these values:
- Create accounts with known initial balances.
- Run successful transfers and check both resulting balances.
- Verify total balance is preserved across the accounts involved.
- Attempt insufficient-funds transfers and verify no partial changes occur.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_client import connect_gateway
from week09_common import GENERATED_DIRECTORY

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import week09_gateway_pb2


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Provided wallet client.")
    parser.add_argument("--gateway", default="127.0.0.1:50151", help="Gateway address in host:port form.")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create")
    create.add_argument("account_id", help="Account identifier.")
    create.add_argument("initial_balance_cents", type=int, help="Initial balance in integer cents.")

    get_account = sub.add_parser("get")
    get_account.add_argument("account_id", help="Account identifier.")

    transfer = sub.add_parser("transfer")
    transfer.add_argument("from_account_id", help="Source account identifier.")
    transfer.add_argument("to_account_id", help="Destination account identifier.")
    transfer.add_argument("amount_cents", type=int, help="Transfer amount in integer cents.")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    stub = connect_gateway(args.gateway)
    if args.command == "create":
        print(stub.CreateAccount(week09_gateway_pb2.CreateAccountRequest(account_id=args.account_id, initial_balance_cents=args.initial_balance_cents)))
    elif args.command == "get":
        print(stub.GetAccount(week09_gateway_pb2.GetAccountRequest(account_id=args.account_id)))
    else:
        print(
            stub.Transfer(
                week09_gateway_pb2.TransferRequest(
                    from_account_id=args.from_account_id,
                    to_account_id=args.to_account_id,
                    amount_cents=args.amount_cents,
                )
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
