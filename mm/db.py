from __future__ import annotations

import enum
import logging
from datetime import datetime, timedelta
from random import randrange, uniform
from typing import Optional, Sequence, Tuple

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UnicodeText,
    and_,
    create_engine,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.event import listen
from sqlalchemy.orm import Session, declarative_base, relationship  # type: ignore
from sqlalchemy.schema import DDL
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.type_api import TypeDecorator

__all__ = ()


Base = declarative_base()


class BadPriorStatusError(ValueError):
    pass


class JobStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    CANCELED = "CANCELED"


class ISO8601DateTypeDecorator(TypeDecorator):
    r"""
    TypeDecorator to do proper parsing of ISO8601 dates with fractional second
    components. See:

    * https://github.com/sqlalchemy/sqlalchemy/issues/7027
    * https://github.com/sqlalchemy/sqlalchemy/issues/7029
    """
    impl = String
    cache_ok = True

    def process_bind_param(self, value: Optional[datetime], dialect) -> Optional[str]:
        if value is not None:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            return None

    def process_result_value(self, value: Optional[str], dialect) -> Optional[datetime]:
        if value is not None:
            return datetime.fromisoformat(value)
        else:
            return None


class DbJob(Base):  # type: ignore
    __tablename__ = "app_jobs"

    job_id = Column(Integer, primary_key=True)
    created_at = Column(
        DateTime().with_variant(ISO8601DateTypeDecorator(), "sqlite"),
        # Zero padding motivated by
        # <https://github.com/sqlalchemy/sqlalchemy/issues/7027>
        server_default=func.STRFTIME("%Y-%m-%d %H:%M:%f000", "NOW"),
        nullable=False,
    )
    next_attempt_at = Column(DateTime, nullable=False)
    status = Column(
        Enum(JobStatus), server_default=JobStatus.PENDING.value, nullable=False
    )
    status_at = Column(
        DateTime,
        # Zero padding motivated by
        # <https://github.com/sqlalchemy/sqlalchemy/issues/7027>
        server_default=func.STRFTIME("%Y-%m-%d %H:%M:%f000", "NOW"),
        nullable=False,
    )
    message = Column(UnicodeText)

    history: Sequence[DbJobHistory] = relationship(  # type: ignore
        "DbJobHistory",
        order_by="DbJobHistory.status_at.desc()",
        back_populates="job",
    )


Index("idx_job_status", DbJob.job_id, DbJob.status)


class DbJobHistory(Base):  # type: ignore
    __tablename__ = "app_job_histories"

    job_history_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("app_jobs.job_id"), nullable=False)
    status = Column(Enum(JobStatus), nullable=False)
    status_at = Column(
        DateTime,
        # Zero padding motivated by
        # <https://github.com/sqlalchemy/sqlalchemy/issues/7027>
        server_default=func.STRFTIME("%Y-%m-%d %H:%M:%f000", "NOW"),
        nullable=False,
    )
    message = Column(UnicodeText)

    job: DbJob = relationship("DbJob", back_populates="history")  # type: ignore


listen(
    DbJobHistory.__table__,
    "after_create",
    DDL(
        """
CREATE TRIGGER append_job_history AFTER UPDATE OF status ON app_jobs
BEGIN
  UPDATE app_jobs
  -- Zero padding motivated by
  -- <https://github.com/sqlalchemy/sqlalchemy/issues/7027>
  SET status_at = STRFTIME('%%Y-%%m-%%d %%H:%%M:%%f000', 'NOW')
  WHERE job_id = new.job_id ;

  INSERT INTO app_job_histories (
    job_id,
    status,
    status_at,
    message
  ) VALUES (
    old.job_id,
    old.status,
    old.status_at,
    old.message
  ) ;
END
"""
    ),
)


def new_engine(url: str):
    return create_engine(
        url,
        connect_args={"check_same_thread": False},
        native_datetime=True,
    )


def db_fake_advance_jobs(
    db: Session,
) -> None:
    r"""
    Simulates past activity by moving [``JobStatus.PENDING``][JobStatus.PENDING] and
    [``JobStatus.RUNNING``][JobStatus.RUNNING] jobs forward until they are no longer
    executable (either because they have been completed, or because they are not
    scheduled to run until later).
    """
    # Keep iterating until we have no more jobs to move forward
    while True:
        now = datetime.utcnow()
        stmt = select(DbJob).where(  # type: ignore
            or_(
                # It's ready to "execute"
                and_(
                    DbJob.status == JobStatus.PENDING,
                    DbJob.next_attempt_at < now - timedelta(seconds=2),
                ),
                # It's been "executing" for awhile
                and_(
                    DbJob.status == JobStatus.RUNNING,
                    DbJob.status_at < now - timedelta(seconds=10),
                ),
            )
        )
        db_jobs = db.execute(stmt).scalars().all()

        if not db_jobs:
            # Stop when all jobs are in a stable state
            break

        for db_job in db_jobs:
            target_status = JobStatus.CANCELED
            target_status_at: Optional[datetime] = None
            target_next_attempt_at: Optional[datetime] = None
            target_message: Optional[str] = None

            try:
                if db_job.status == JobStatus.PENDING:
                    target_status_at = db_job.next_attempt_at + timedelta(
                        seconds=uniform(0.25, 1.5)
                    )
                    assert target_status_at is not None
                    target_status = JobStatus.RUNNING
                elif db_job.status == JobStatus.RUNNING:
                    target_status_at = db_job.status_at + timedelta(
                        seconds=uniform(5, 15)
                    )
                    assert target_status_at is not None
                    d10 = randrange(0, 10, 1)

                    # 10% are permanent failures
                    if d10 == 0:
                        target_status = JobStatus.DONE
                        target_message = "Permanent failure"
                    # 30% are temporary failures
                    elif d10 in (1, 2, 3):
                        target_status = JobStatus.PENDING
                        target_next_attempt_at = target_status_at + timedelta(
                            seconds=10
                        )
                        assert target_next_attempt_at is not None
                        target_message = "Temporary failure"
                    # The rest are successes
                    else:
                        target_status = JobStatus.DONE
                else:
                    logging.warning(
                        f"unexpected job status {db_job.status} for {db_job.job_id}"
                    )
                    continue

                _update_job_status(
                    db,
                    db_job,
                    status=target_status,
                    next_attempt_at=target_next_attempt_at,
                    message=target_message,
                )
                db.commit()

                if target_status_at:
                    db_job.status_at = target_status_at
                    db.commit()
                    db.refresh(db_job)
                    assert db_job.status_at == target_status_at
            except BadPriorStatusError:
                # Log, but otherwise ignore all failures
                logging.warning(
                    f"unable to update status to {target_status.value} for {db_job.job_id}"
                )


def db_insert_job(
    db: Session,
    next_attempt_at: Optional[datetime] = None,
    message: Optional[str] = None,
) -> DbJob:
    next_attempt_at = (
        next_attempt_at
        if next_attempt_at
        # Zero padding motivated by
        # <https://github.com/sqlalchemy/sqlalchemy/issues/7027>
        else func.STRFTIME("%Y-%m-%d %H:%M:%f000", "NOW")
    )
    db_job = DbJob(next_attempt_at=next_attempt_at, message=message)
    db.add(db_job)
    db.commit()
    db.refresh(db_job)

    return db_job


def db_select_in_flight_counts(db: Session) -> Tuple[int, int]:
    pending_count = db.execute(
        text(
            f"""
SELECT COUNT(*) AS pending_count
FROM {DbJob.__tablename__}
WHERE status = '{JobStatus.PENDING.value}'
"""
        )
    ).first()
    running_count = db.execute(
        text(
            f"""
SELECT COUNT(*) AS running_count
FROM {DbJob.__tablename__}
WHERE status = '{JobStatus.RUNNING.value}'
"""
        )
    ).first()

    return pending_count["pending_count"], running_count["running_count"]


def db_select_job(db: Session, job_id: int) -> DbJob:
    stmt = select(DbJob).where(DbJob.job_id == job_id)  # type: ignore
    db_job = db.execute(stmt).scalar_one()

    return db_job


def db_select_job_histories(
    db: Session,
    job_id: int,
    skip: int = 0,
    limit: int = 100,
) -> Sequence[DbJobHistory]:
    stmt = select(DbJobHistory).where(DbJobHistory.job_id == job_id)  # type: ignore
    stmt = (
        stmt.order_by(DbJobHistory.status_at.desc(), DbJobHistory.job_history_id.desc())
        .offset(skip)
        .limit(limit)
    )
    db_job_histories = db.execute(stmt).scalars()

    return list(db_job_histories)


def db_select_jobs(
    db: Session,
    status: Optional[JobStatus] = None,
    skip: int = 0,
    limit: int = 100,
) -> Sequence[DbJob]:
    stmt = select(DbJob)  # type: ignore

    if status:
        stmt = stmt.where(DbJob.status == status)

    stmt = (
        stmt.order_by(DbJob.created_at.desc(), DbJob.job_id.desc())
        .offset(skip)
        .limit(limit)
    )
    db_jobs = db.execute(stmt).scalars()

    return list(db_jobs)


def db_update_job_status(
    db: Session,
    db_job: DbJob,
    status: JobStatus,
    next_attempt_at: Optional[datetime] = None,
    message: Optional[str] = None,
) -> None:
    message = message if message else ""

    try:
        _update_job_status(db, db_job, status, next_attempt_at, message)
    except BadPriorStatusError:
        db.rollback()

        raise
    else:
        db.commit()
        db.refresh(db_job)


def _update_job_status(
    db: Session,
    db_job: DbJob,
    status: JobStatus,
    next_attempt_at: Optional[datetime],
    message: Optional[str],
) -> None:
    message = message if message else ""

    if status == JobStatus.PENDING:
        prior_status = JobStatus.RUNNING
    elif status == JobStatus.RUNNING:
        prior_status = JobStatus.PENDING
    elif status == JobStatus.DONE:
        prior_status = JobStatus.RUNNING
    elif status == JobStatus.CANCELED:
        prior_status = JobStatus.PENDING

    stmt = (
        update(DbJob)  # type: ignore
        .where(
            and_(
                DbJob.job_id == db_job.job_id,
                DbJob.status == prior_status,
            )
        )
        .values(status=status)
    )
    res = db.execute(stmt)

    if res.rowcount < 1:
        db.refresh(db_job)

        raise BadPriorStatusError

    db_job.message = message

    if next_attempt_at:
        db_job.next_attempt_at = next_attempt_at
