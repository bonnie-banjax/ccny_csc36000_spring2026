from __future__ import annotations

import pytest


pytestmark = pytest.mark.usefixtures("cluster")


def test_registration_atomicity_and_reads(selected_application, gateway_stub):
    if selected_application != "course_registration":
        pytest.skip("registration tests only run when SELECTED_APPLICATION == 'course_registration'")

    gateway_pb2, stub = gateway_stub
    stub.CreateSection(gateway_pb2.CreateSectionRequest(section_id="CSC36000-01", capacity=1), timeout=5.0)
    enroll_response = stub.Enroll(gateway_pb2.EnrollRequest(student_id="student-a", section_id="CSC36000-01"), timeout=5.0)
    assert enroll_response.committed is True
    assert enroll_response.enrolled_count == 1

    schedule = stub.GetStudentSchedule(gateway_pb2.GetStudentScheduleRequest(student_id="student-a"), timeout=5.0)
    roster = stub.GetSectionRoster(gateway_pb2.GetSectionRosterRequest(section_id="CSC36000-01"), timeout=5.0)
    assert "CSC36000-01" in schedule.section_ids
    assert "student-a" in roster.student_ids


def test_registration_isolation_last_seat(selected_application, gateway_stub):
    if selected_application != "course_registration":
        pytest.skip("registration tests only run when SELECTED_APPLICATION == 'course_registration'")

    gateway_pb2, stub = gateway_stub
    stub.CreateSection(gateway_pb2.CreateSectionRequest(section_id="CSC36000-02", capacity=1), timeout=5.0)
    first = stub.Enroll(gateway_pb2.EnrollRequest(student_id="student-1", section_id="CSC36000-02"), timeout=5.0)
    assert first.committed is True

    with pytest.raises(Exception):
        stub.Enroll(gateway_pb2.EnrollRequest(student_id="student-2", section_id="CSC36000-02"), timeout=5.0)

    roster = stub.GetSectionRoster(gateway_pb2.GetSectionRosterRequest(section_id="CSC36000-02"), timeout=5.0)
    assert roster.student_ids == ["student-1"]


def test_registration_unhappy_path_duplicate_enrollment_keeps_state_consistent(selected_application, gateway_stub):
    if selected_application != "course_registration":
        pytest.skip("registration tests only run when SELECTED_APPLICATION == 'course_registration'")

    gateway_pb2, stub = gateway_stub
    stub.CreateSection(gateway_pb2.CreateSectionRequest(section_id="CSC36000-03", capacity=2), timeout=5.0)
    first = stub.Enroll(gateway_pb2.EnrollRequest(student_id="student-repeat", section_id="CSC36000-03"), timeout=5.0)
    assert first.committed is True

    with pytest.raises(Exception):
        stub.Enroll(
            gateway_pb2.EnrollRequest(student_id="student-repeat", section_id="CSC36000-03"),
            timeout=5.0,
        )

    schedule = stub.GetStudentSchedule(
        gateway_pb2.GetStudentScheduleRequest(student_id="student-repeat"),
        timeout=5.0,
    )
    roster = stub.GetSectionRoster(gateway_pb2.GetSectionRosterRequest(section_id="CSC36000-03"), timeout=5.0)
    assert schedule.section_ids.count("CSC36000-03") == 1
    assert roster.student_ids.count("student-repeat") == 1
