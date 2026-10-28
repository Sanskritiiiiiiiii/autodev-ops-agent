import asyncio
import logging
import os
import socket

import redis.asyncio as aioredis

from app.agent.llm_groq import make_groq_llm
from app.config import REDIS_HOST, REDIS_PORT
from app.queue.triage_handler import make_triage_handler
from app.queue.worker import Worker
from app.search.embedder import Embedder
from app.search.reranker import Reranker


async def main() -> None:
    llm = make_groq_llm()  # fails fast if GROQ_API_KEY / GROQ_MODEL are missing
    logging.getLogger("autodev.worker").info("loading embedding and rerank models...")
    embedder, reranker = Embedder(), Reranker()

    r = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    worker = Worker(
        r, make_triage_handler(r, llm, embedder, reranker),
        consumer=f"{socket.gethostname()}-{os.getpid()}",
    )
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