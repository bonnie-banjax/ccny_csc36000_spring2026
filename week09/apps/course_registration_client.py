#!/usr/bin/env python3
from __future__ import annotations

"""
Provided course registration application client.

Application model:
- student_id: identifies one student
- section_id: identifies one course section, for example CSC36000-01
- capacity: maximum allowed enrollment for a section

Provided operations:
- create-section section_id capacity
- enroll student_id section_id
- drop student_id section_id
- schedule student_id
- roster section_id

What the application expects from the student implementation:
- A student cannot be enrolled in the same section more than once.
- A section cannot exceed its seat capacity.
- Enrollment must update all related state atomically.
- Reads of student schedule and section roster must agree.
- Conflicting enrollments for the last seat must not overbook.

What the tests do with these values:
- Create sections before attempting enrollment.
- Enroll a student, then read both schedule and roster.
- Attempt conflicting enrollment when only one seat remains.
- Attempt repeated enrollment of the same student in the same section.
- Verify failed enrollment attempts do not leave partial state behind.
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
    parser = argparse.ArgumentParser(description="Provided course registration client.")
    parser.add_argument("--gateway", default="127.0.0.1:50151", help="Gateway address in host:port form.")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-section")
    create.add_argument("section_id", help="Section identifier such as CSC36000-01.")
    create.add_argument("capacity", type=int, help="Maximum number of students allowed in the section.")

    enroll = sub.add_parser("enroll")
    enroll.add_argument("student_id", help="Student identifier.")
    enroll.add_argument("section_id", help="Section identifier.")

    drop = sub.add_parser("drop")
    drop.add_argument("student_id", help="Student identifier.")
    drop.add_argument("section_id", help="Section identifier.")

    schedule = sub.add_parser("schedule")
    schedule.add_argument("student_id", help="Student identifier.")

    roster = sub.add_parser("roster")
    roster.add_argument("section_id", help="Section identifier.")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    stub = connect_gateway(args.gateway)
    if args.command == "create-section":
        print(stub.CreateSection(week09_gateway_pb2.CreateSectionRequest(section_id=args.section_id, capacity=args.capacity)))
    elif args.command == "enroll":
        print(stub.Enroll(week09_gateway_pb2.EnrollRequest(student_id=args.student_id, section_id=args.section_id)))
    elif args.command == "drop":
        print(stub.Drop(week09_gateway_pb2.DropRequest(student_id=args.student_id, section_id=args.section_id)))
    elif args.command == "schedule":
        print(stub.GetStudentSchedule(week09_gateway_pb2.GetStudentScheduleRequest(student_id=args.student_id)))
    else:
        print(stub.GetSectionRoster(week09_gateway_pb2.GetSectionRosterRequest(section_id=args.section_id)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
