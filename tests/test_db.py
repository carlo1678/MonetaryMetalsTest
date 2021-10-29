from __future__ import annotations

from datetime import datetime, timedelta
from operator import attrgetter
from typing import Set

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from mm.db import (
    BadPriorStatusError,
    Base,
    DbJob,
    JobStatus,
    db_fake_advance_jobs,
    db_insert_job,
    db_select_in_flight_counts,
    db_select_job_histories,
    db_select_jobs,
    db_update_job_status,
    new_engine,
)

IN_MEMORY_SQLITE_URL = "sqlite://"


@pytest.fixture(name="db")
def db_fixture():
    engine = new_engine(IN_MEMORY_SQLITE_URL)

    with Session(engine) as db:  # type: ignore
        # See <https://github.com/tiangolo/fastapi/issues/3906>
        Base.metadata.create_all(db.connection().engine)
        yield db


def test_fake_advance_jobs(
    db: Session,  # pytest fixture
) -> None:
    now = datetime.utcnow()
    ready_db_job = db_insert_job(db, next_attempt_at=now - timedelta(seconds=3))
    pending_db_job = db_insert_job(db, next_attempt_at=now + timedelta(days=5))
    running_db_job = db_insert_job(db)
    running_db_job.status_at = now - timedelta(seconds=21)
    running_db_job.status = JobStatus.RUNNING  # type: ignore
    db.commit()
    # Override status_at in a separate commit to avoid the trigger
    running_db_job.status_at = now - timedelta(seconds=11)
    db.commit()

    db.refresh(running_db_job)
    db.refresh(ready_db_job)
    db.refresh(pending_db_job)
    assert running_db_job.status == JobStatus.RUNNING
    assert ready_db_job.status == JobStatus.PENDING
    assert pending_db_job.status == JobStatus.PENDING

    db_fake_advance_jobs(db)

    db.refresh(running_db_job)
    db.refresh(ready_db_job)
    db.refresh(pending_db_job)
    assert running_db_job.status != JobStatus.RUNNING
    assert ready_db_job.status == JobStatus.RUNNING
    assert pending_db_job.status == JobStatus.PENDING


def test_select_in_flight_counts(
    db: Session,  # pytest fixture
) -> None:
    for i in range(48):
        job = db_insert_job(db)

        if i % 4 == 0:
            db_update_job_status(db, job, status=JobStatus.CANCELED)
        elif i % 3 == 0:
            db_update_job_status(db, job, status=JobStatus.RUNNING)
            db_update_job_status(db, job, status=JobStatus.DONE)
        elif i % 2 == 0:
            db_update_job_status(db, job, status=JobStatus.RUNNING)

    pending_count, running_count = db_select_in_flight_counts(db)
    assert pending_count == 16
    assert running_count == 8


def test_select_job(
    db: Session,  # pytest fixture
) -> None:
    now = datetime.utcnow()

    job = db_insert_job(db)
    db_update_job_status(
        db,
        job,
        status=JobStatus.RUNNING,
        message="running",
    )
    db_update_job_status(
        db,
        job,
        status=JobStatus.PENDING,
        next_attempt_at=now + timedelta(days=5),
        message="pending again",
    )
    db_update_job_status(
        db,
        job,
        status=JobStatus.CANCELED,
        message="canceled",
    )
    jobs = db.execute(select(DbJob)).scalars().all()  # type: ignore
    assert len(jobs) == 1, jobs

    (job,) = jobs
    assert job.status == JobStatus.CANCELED
    assert job.next_attempt_at == now + timedelta(days=5)
    assert job.message == "canceled"

    history = list(job.history)
    assert len(history) == 3, history

    history.sort(key=attrgetter("status_at"))
    pending1, running, pending2 = history
    assert pending1.status == JobStatus.PENDING
    assert not pending1.message
    assert running.status == JobStatus.RUNNING
    assert running.message == "running"
    assert pending2.status == JobStatus.PENDING
    assert pending2.message == "pending again"


def test_select_job_histories(
    db: Session,  # pytest fixture
) -> None:
    db_job = db_insert_job(db)

    for i in range(205):
        db_update_job_status(
            db,
            db_job,
            status=JobStatus.PENDING if i % 2 else JobStatus.RUNNING,
        )

    db_job_histories1 = db_select_job_histories(db, db_job.job_id, skip=0)
    db_job_histories2 = db_select_job_histories(db, db_job.job_id, skip=100)
    db_job_histories3 = db_select_job_histories(db, db_job.job_id, skip=200)

    assert len(db_job_histories1) == 100
    assert len(db_job_histories2) == 100
    assert len(db_job_histories3) == 5
    job_history_ids: Set[int] = set()
    job_history_ids.update(
        db_job_history.job_history_id
        for db_job_histories in (
            db_job_histories1,
            db_job_histories2,
            db_job_histories3,
        )
        for db_job_history in db_job_histories
    )
    assert len(job_history_ids) == 205


def test_select_jobs(
    db: Session,  # pytest fixture
) -> None:
    job_ids: Set[int] = set()

    for i in range(205):
        db_job = db_insert_job(db)
        job_ids.add(db_job.job_id)

    db.commit()
    db_jobs1 = db_select_jobs(db, skip=0)
    db_jobs2 = db_select_jobs(db, skip=100)
    db_jobs3 = db_select_jobs(db, skip=200)
    assert len(db_jobs1) == 100
    assert len(db_jobs2) == 100
    assert len(db_jobs3) == 5

    found_job_ids: Set[int] = set()
    found_job_ids.update(
        db_job.job_id
        for db_jobs in (
            db_jobs1,
            db_jobs2,
            db_jobs3,
        )
        for db_job in db_jobs
    )
    assert found_job_ids == job_ids


def test_select_jobs_status(
    db: Session,  # pytest fixture
) -> None:
    job_ids: Set[int] = set()

    for i in range(205):
        db_job = db_insert_job(db)
        job_ids.add(db_job.job_id)

        if i % 2:
            db_update_job_status(
                db,
                db_job,
                status=JobStatus.RUNNING,
            )

    db_jobs_pending1 = db_select_jobs(db, status=JobStatus.PENDING, skip=0)
    db_jobs_pending2 = db_select_jobs(db, status=JobStatus.PENDING, skip=100)
    db_jobs_pending3 = db_select_jobs(db, status=JobStatus.PENDING, skip=200)
    db_jobs_running1 = db_select_jobs(db, status=JobStatus.RUNNING, skip=0)
    db_jobs_running2 = db_select_jobs(db, status=JobStatus.RUNNING, skip=100)
    db_jobs_running3 = db_select_jobs(db, status=JobStatus.RUNNING, skip=200)
    db_jobs_canceled = db_select_jobs(db, status=JobStatus.CANCELED, skip=0)
    assert len(db_jobs_pending1) == 100
    assert len(db_jobs_pending2) == 3
    assert len(db_jobs_pending3) == 0
    assert len(db_jobs_running1) == 100
    assert len(db_jobs_running2) == 2
    assert len(db_jobs_running3) == 0
    assert len(db_jobs_canceled) == 0


def test_update_job_status(
    db: Session,  # pytest fixture
) -> None:
    db_job = db_insert_job(db)
    assert db_job.status == JobStatus.PENDING
    db_update_job_status(db, db_job, JobStatus.RUNNING)

    with pytest.raises(BadPriorStatusError):
        db_update_job_status(db, db_job, JobStatus.CANCELED)
