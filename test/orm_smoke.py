"""
ORM smoke tests for yashandb+yaspy.

Run:

  1) Edit YASHANDB_URL in this file
  2) python test/orm_smoke.py

This script is intentionally lightweight (no pytest dependency) and focuses on:
- ORM CRUD + Session/transaction behavior
- One-to-many relationship loading (lazy + selectin)
- Common SQL functions via sqlalchemy.func
- Common ORM query expressions (join/subquery/exists/group_by)
"""

from __future__ import annotations

import traceback
from typing import List, Optional

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload


#YASHANDB_URL = "yashandb+yaspy://USER:PASSWORD@HOST:PORT/DBNAME?schema=MY_TEST001"
YASHANDB_URL = "yashandb+yaspy://MY_TEST001:123456@172.16.90.87:1688/test"

def _file_url() -> str:
    url = (YASHANDB_URL or "").strip()
    if not url or "USER:PASSWORD@HOST:PORT" in url:
        raise RuntimeError(
            "Please edit test/orm_smoke.py and set YASHANDB_URL to a real "
            "connection string before running."
        )
    return url


def _print_banner(msg: str) -> None:
    print(f"\n=== {msg} ===")


Base = declarative_base()


class User(Base):
    __tablename__ = "orm_smoke_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True)
    created_at = Column(DateTime, server_default=func.current_timestamp())

    addresses = relationship(
        "Address",
        back_populates="user", cascade="all, delete-orphan"
    )


class Address(Base):
    __tablename__ = "orm_smoke_addresses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("orm_smoke_users.id"), nullable=False)
    email = Column(String(100), nullable=False)

    user = relationship("User", back_populates="addresses")


def _setup_db(engine: Engine) -> None:
    _print_banner("create_all")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def _teardown_db(engine: Engine) -> None:
    _print_banner("drop_all")
    Base.metadata.drop_all(engine)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_orm_crud_and_relationships(engine: Engine) -> None:
    _print_banner("ORM CRUD + relationships")
    with Session(engine) as s:
        u1 = User(name="u1")
        u1.addresses.append(Address(email="u1@example.com"))
        u1.addresses.append(Address(email="u1+2@example.com"))
        s.add(u1)
        s.commit()

        # Identity / refresh
        _assert(u1.id is not None and u1.id > 0, "autoincrement id not assigned")

        # Lazy load
        s.expire(u1, ["addresses"])
        _assert(len(u1.addresses) == 2, "lazy relationship load failed")

        # selectinload eager load
        users = (
            s.execute(
                select(User)
                .options(selectinload(User.addresses))
                .order_by(User.id)
            )
            .scalars()
            .all()
        )
        _assert(len(users) == 1, "expected 1 user")
        _assert(len(users[0].addresses) == 2, "selectinload failed")

        # Delete cascade
        s.delete(users[0])
        s.commit()
        remaining_addrs = s.execute(select(func.count(Address.id))).scalar()
        _assert(remaining_addrs == 0, "delete-orphan cascade failed")


def test_transactions_and_rollback(engine: Engine) -> None:
    _print_banner("transaction + rollback")
    with Session(engine) as s:
        s.add(User(name="tx1"))
        s.flush()

        # Rollback should discard pending rows
        s.rollback()
        n = s.execute(select(func.count(User.id))).scalar()
        _assert(n == 0, "rollback did not discard flushed row")


def test_integrity_error(engine: Engine) -> None:
    _print_banner("IntegrityError mapping (unique constraint)")
    with Session(engine) as s:
        s.add(User(name="uniq"))
        s.commit()

        s.add(User(name="uniq"))
        try:
            s.commit()
        except IntegrityError:
            s.rollback()
        else:
            raise AssertionError("expected IntegrityError for duplicate unique key")


def test_orm_query_expressions_and_functions(engine: Engine) -> None:
    _print_banner("ORM query expressions + common func.*")
    with Session(engine) as s:
        u1 = User(name="q1", addresses=[Address(email="a@ex.com")])
        u2 = User(name="q2", addresses=[Address(email="b@ex.com"), Address(email="c@ex.com")])
        s.add_all([u1, u2])
        s.commit()

        # func.count + group_by + join
        rows = s.execute(
            select(User.name, func.count(Address.id))
            .join(Address)
            .group_by(User.name)
            .order_by(User.name)
        ).all()
        _assert(rows == [("q1", 1), ("q2", 2)], f"unexpected group_by rows: {rows!r}")

        # exists() correlated subquery
        addr_exists = select(Address.id).where(Address.user_id == User.id).exists()
        names = (
            s.execute(select(User.name).where(addr_exists).order_by(User.name))
            .scalars()
            .all()
        )
        _assert(names == ["q1", "q2"], f"unexpected exists names: {names!r}")

        # scalar subquery (count addresses per user)
        addr_count_sq = (
            select(func.count(Address.id))
            .where(Address.user_id == User.id)
            .scalar_subquery()
        )
        rows2 = s.execute(
            select(User.name, addr_count_sq.label("addr_cnt")).order_by(User.name)
        ).all()
        _assert(rows2 == [("q1", 1), ("q2", 2)], f"unexpected scalar subquery rows: {rows2!r}")

        # String/SQL functions commonly used
        # - length, lower/upper, coalesce, concat (|| via + for SQLAlchemy strings)
        rows3 = s.execute(
            select(
                User.name,
                func.length(User.name),
                func.lower(User.name),
                func.upper(User.name),
                func.coalesce(User.name, "x"),
                (User.name + "_sfx"),
            ).order_by(User.name)
        ).all()
        _assert(rows3[0][0] == "q1", "unexpected ordering for func query")
        _assert(rows3[0][1] == 2, "func.length mismatch")
        _assert(rows3[0][2] == "q1", "func.lower mismatch")
        _assert(rows3[0][3] == "Q1", "func.upper mismatch")
        _assert(rows3[0][4] == "q1", "func.coalesce mismatch")
        _assert(rows3[0][5] == "q1_sfx", "string concat mismatch")

        # current_timestamp round-trip (non-null)
        ts = s.execute(select(func.current_timestamp())).scalar()
        _assert(ts is not None, "current_timestamp returned None")


def main() -> int:
    from sqlalchemy import create_engine

    url = _file_url()
    _print_banner("connect")
    # avoid printing credentials; keep it simple and robust
    print("Using URL from env: [masked]")
    engine = create_engine(url, future=True)

    try:
        # Keep each test isolated so they don't interfere with each other's
        # row expectations (e.g. unique-key tests inserting extra rows).
        tests = [
            test_orm_crud_and_relationships,
            test_transactions_and_rollback,
            test_integrity_error,
            test_orm_query_expressions_and_functions,
        ]

        for fn in tests:
            _setup_db(engine)
            try:
                fn(engine)
            finally:
                try:
                    _teardown_db(engine)
                except Exception:
                    pass
    except Exception as e:
        _print_banner("FAILED")
        print(f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 1

    _print_banner("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

