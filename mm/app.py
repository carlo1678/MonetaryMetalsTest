from __future__ import annotations

import logging
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar, cast

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, root_validator
from sqlalchemy.exc import NoResultFound  # type: ignore
from sqlalchemy.orm import Session, sessionmaker

from fastapi.middleware.cors import CORSMiddleware

from .db import (
    BadPriorStatusError,
    Base,
    DbJob,
    DbJobHistory,
    JobStatus,
    db_fake_advance_jobs,
    db_insert_job,
    db_select_in_flight_counts,
    db_select_job,
    db_select_job_histories,
    db_select_jobs,
    db_update_job_status,
    new_engine,
)

__all__ = ("app",)

_T = TypeVar("_T", bound=Callable)

engine = new_engine("sqlite:///./jobs.db")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)
app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins
)

class SystemSummary(BaseModel):
    pending_count: int
    running_count: int


class JobIdErrorMessage(BaseModel):
    job_id: int
    message: str


class JobBase(BaseModel):
    status: JobStatus
    status_at: datetime
    message: str

    @root_validator(pre=True)
    def ensure_message(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values = dict(values)
        values["message"] = values["message"] if values.get("message") else ""

        return values


class Job(JobBase):
    job_id: int
    created_at: datetime
    next_attempt_at: Optional[datetime]

    class Config:
        orm_mode = True

    @root_validator
    def blank_next_attempt_at(cls, values: Dict[str, Any]) -> Dict[str, Any]:

        if values["status"] != JobStatus.PENDING:
            values = dict(values)
            values["next_attempt_at"] = None

        return values


class JobErrorMessage(BaseModel):
    job: Job
    message: str


class JobPriorStatus(JobBase):
    job_history_id: int

    class Config:
        orm_mode = True


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


def _fake_advance_jobs_hack(func: _T) -> _T:
    r"""
    Decorator to call [``db_fake_advance_jobs``][mm.db.db_fake_advance_jobs] before
    calling ``#!python func``..
    """

    @wraps(func)
    def _wrapped(*, db: Session = Depends(get_db), **kw):
        if isinstance(db, Session):
            try:
                db_fake_advance_jobs(db)
            except:  # noqa: E722
                logging.exception("failed to advance database")

        return func(db=db, **kw)

    return cast(_T, _wrapped)


@app.get("/", response_model=SystemSummary)
@_fake_advance_jobs_hack
def summary(*, db: Session = Depends(get_db)) -> SystemSummary:
    pending_count, running_count = db_select_in_flight_counts(db)

    return SystemSummary(pending_count=pending_count, running_count=running_count)


@app.get(
    "/job/{job_id}",
    response_model=Job,
    responses={
        status.HTTP_404_NOT_FOUND: {"model": JobIdErrorMessage},
    },
)
@_fake_advance_jobs_hack
def job(*, job_id: int, db: Session = Depends(get_db)) -> DbJob:
    try:
        db_job = db_select_job(db, job_id)
    except NoResultFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=JobIdErrorMessage(job_id=job_id, message="job not found").dict(),
        )
    else:
        return db_job


@app.get(
    "/job/{job_id}/history",
    response_model=Sequence[JobPriorStatus],
    responses={
        status.HTTP_404_NOT_FOUND: {"model": JobIdErrorMessage},
    },
)
@_fake_advance_jobs_hack
def job_history(
    *,
    job_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    db: Session = Depends(get_db),
) -> Sequence[DbJobHistory]:
    try:
        db_select_job(db, job_id)
    except NoResultFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=JobIdErrorMessage(job_id=job_id, message="job not found").dict(),
        )
    else:
        return db_select_job_histories(db, job_id, skip, limit)


@app.post(
    "/job/cancel/{job_id}",
    response_model=Job,
    responses={
        status.HTTP_404_NOT_FOUND: {"model": JobIdErrorMessage},
        status.HTTP_409_CONFLICT: {"model": JobErrorMessage},
    },
)
@_fake_advance_jobs_hack
def cancel_job(
    *,
    job_id: int,
    next_attempt_at: Optional[datetime] = None,
    message: Optional[str] = None,
    db: Session = Depends(get_db),
) -> DbJob:
    try:
        db_job = db_select_job(db, job_id)
        db_update_job_status(
            db,
            db_job,
            next_attempt_at=next_attempt_at,
            status=JobStatus.CANCELED,
            message=message,
        )
    except NoResultFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=JobIdErrorMessage(job_id=job_id, message="job not found").dict(),
        )
    except BadPriorStatusError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=jsonable_encoder(
                JobErrorMessage(
                    job=Job.from_orm(db_job),
                    message=f"unable to cancel job in {db_job.status.value} state",  # type: ignore
                )
            ),
        )
    else:
        return db_job


@app.post("/job/new", response_model=Job)
@_fake_advance_jobs_hack
def new_job(
    *,
    next_attempt_at: Optional[datetime] = None,
    message: Optional[str] = None,
    db: Session = Depends(get_db),
) -> DbJob:
    return db_insert_job(db, next_attempt_at, message)


@app.get("/jobs", response_model=Sequence[Job])
@_fake_advance_jobs_hack
def jobs(
    *,
    status: Optional[JobStatus] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    db: Session = Depends(get_db),
) -> Sequence[DbJob]:
    return db_select_jobs(db, status, skip, limit)
