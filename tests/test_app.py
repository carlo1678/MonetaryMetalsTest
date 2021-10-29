from __future__ import annotations

import os
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from typing import Dict, Set

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from mm.app import Base, app, get_db
from mm.db import new_engine


@pytest.fixture(name="db")
def db_fixture():
    f = NamedTemporaryFile(prefix=__name__, suffix=".db", delete=False)
    db_path = f.name
    f.close()
    # Yes, this creates a race condition, but SQLite chokes on an existing, empty file
    os.remove(db_path)
    engine = new_engine(f"sqlite:///{db_path}")
    # See <https://github.com/tiangolo/fastapi/issues/3906>
    Base.metadata.create_all(engine)

    try:
        with Session(engine) as db:  # type: ignore
            yield db
    finally:
        try:
            os.remove(db_path)
        except:  # noqa: E722
            pass


@pytest.fixture(name="client")
def client_fixture(
    db: Session,  # pytest fixture
):
    def get_db_override():
        return db

    app.dependency_overrides[get_db] = get_db_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_summary(
    client: TestClient,  # pytest fixture
) -> None:
    res = client.get("/")
    assert res.status_code == 200
    assert res.json() == {
        "pending_count": 0,
        "running_count": 0,
    }


def test_summary_advance(
    client: TestClient,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    res = client.post(
        "/job/new", params={"next_attempt_at": (now - timedelta(seconds=3)).isoformat()}
    )
    assert res.status_code == 200

    res = client.post(
        "/job/new", params={"next_attempt_at": (now + timedelta(days=1)).isoformat()}
    )
    assert res.status_code == 200

    res = client.get("/")
    assert res.status_code == 200
    assert res.json() == {
        "pending_count": 1,
        "running_count": 1,
    }


def test_job_history(
    client: TestClient,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    res = client.post(
        "/job/new",
        params={
            "message": "I created this",
            "next_attempt_at": (now + timedelta(days=1)).isoformat(),
        },
    )
    assert res.status_code == 200
    new_job = res.json()
    job_id = new_job["job_id"]

    res = client.post(f"/job/cancel/{job_id}", params={"message": "I canceled this"})
    assert res.status_code == 200

    res = client.get(f"/job/{job_id}/history")
    assert res.status_code == 200
    job_history = res.json()
    assert len(job_history) == 1
    (prior_status,) = job_history
    assert prior_status["status"] == new_job["status"] == "PENDING"
    assert prior_status["status_at"] == new_job["status_at"]
    assert prior_status["message"] == new_job["message"] == "I created this"


def test_cancel_job(
    client: TestClient,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    res = client.post(
        "/job/new", params={"next_attempt_at": (now + timedelta(days=1)).isoformat()}
    )
    assert res.status_code == 200
    new_job = res.json()
    job_id = new_job["job_id"]

    res = client.post(f"/job/cancel/{job_id}", params={"message": "I canceled this"})
    assert res.status_code == 200
    canceled_job = res.json()
    assert canceled_job["status"] == "CANCELED"
    assert canceled_job["message"] == "I canceled this"
    assert canceled_job["next_attempt_at"] is None
    assert new_job["created_at"] == canceled_job["created_at"]


def test_new_job(
    client: TestClient,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    next_attempt_at = now + timedelta(days=1)
    res = client.post(
        "/job/new", params={"next_attempt_at": (next_attempt_at).isoformat()}
    )
    assert res.status_code == 200
    new_job = res.json()
    job_id = new_job["job_id"]

    res = client.get(f"/job/{job_id}")
    assert res.status_code == 200
    found_job = res.json()
    assert found_job["status"] == "PENDING"
    assert datetime.fromisoformat(found_job["next_attempt_at"]) == next_attempt_at
    assert found_job["message"] == ""
    assert found_job == new_job


def test_jobs(
    client: TestClient,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    next_attempt_at = now + timedelta(days=1)
    messages_by_job_id: Dict[int, str] = {}

    for i in range(10):
        message = f"{i}"
        res = client.post(
            "/job/new",
            params={
                "message": message,
                "next_attempt_at": (next_attempt_at).isoformat(),
            },
        )
        assert res.status_code == 200
        job_id = res.json()["job_id"]
        messages_by_job_id[job_id] = message

    res = client.get("/jobs")
    assert res.status_code == 200
    jobs = res.json()
    assert len(jobs) == 10

    for job in jobs:
        assert job["status"] == "PENDING"
        assert job["message"] == messages_by_job_id[job["job_id"]]
        assert datetime.fromisoformat(job["next_attempt_at"]) == next_attempt_at


def test_jobs_status(
    client: TestClient,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    next_attempt_at = now + timedelta(days=1)

    for i in range(10):
        res = client.post(
            "/job/new",
            params={
                "next_attempt_at": (next_attempt_at).isoformat(),
            },
        )
        assert res.status_code == 200
        job_id = res.json()["job_id"]

        if i % 2:
            res = client.post(f"/job/cancel/{job_id}")
            assert res.status_code == 200

    res = client.get("/jobs", params={"status": "CANCELED", "limit": 3})
    assert res.status_code == 200
    jobs = res.json()
    assert len(jobs) == 3
    canceled_job_ids: Set[int] = set()

    for job in jobs:
        assert job["status"] == "CANCELED"
        assert job["job_id"] not in canceled_job_ids
        canceled_job_ids.add(job["job_id"])

    res = client.get("/jobs", params={"status": "CANCELED", "skip": 3, "limit": 100})
    assert res.status_code == 200
    jobs = res.json()
    assert len(jobs) == 2

    for job in jobs:
        assert job["status"] == "CANCELED"
        assert job["job_id"] not in canceled_job_ids
        canceled_job_ids.add(job["job_id"])

    res = client.get("/jobs", params={"status": "PENDING"})
    assert res.status_code == 200
    jobs = res.json()
    assert len(jobs) == 5

    for job in jobs:
        assert job["status"] == "PENDING"
