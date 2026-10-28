import asyncio
import logging
import os
import socket

import redis.asyncio as aioredis

from app.config import REDIS_HOST, REDIS_PORT
from app.queue.handlers import placeholder_handler
from app.queue.worker import Worker


async def main() -> None:
    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    worker = Worker(r, placeholder_handler, consumer=f"{socket.gethostname()}-{os.getpid()}")
    try:
        await worker.run()
    finally:
        await r.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("worker stopped")