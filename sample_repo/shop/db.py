import time

ERR_DB_TIMEOUT = "ERR_DB_TIMEOUT"


class DatabaseTimeout(Exception):
    """Raised when a query exceeds its deadline."""


class ConnectionPool:
    """Tiny fake connection pool used for the demo."""

    max_size = 5

    def __init__(self, dsn):
        self.dsn = dsn
        self.in_use = 0

    def acquire(self, timeout=1.0):
        deadline = time.monotonic() + timeout
        while self.in_use >= self.max_size:
            if time.monotonic() > deadline:
                raise DatabaseTimeout(ERR_DB_TIMEOUT)
            time.sleep(0.01)
        self.in_use += 1
        return self

    def release(self):
        self.in_use -= 1


def run_query(pool, sql):
    conn = pool.acquire(timeout=0.5)
    try:
        return f"executed: {sql}"
    finally:
        conn.release()