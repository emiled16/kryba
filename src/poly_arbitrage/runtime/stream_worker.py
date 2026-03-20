from __future__ import annotations

import asyncio

from poly_arbitrage.runtime.bootstrap import build_container


async def _run() -> None:
    container = build_container()
    try:
        stream_job_names = container.ingestion.list_stream_jobs()
        while True:
            if not stream_job_names:
                await asyncio.sleep(5)
                continue
            tasks = [
                asyncio.create_task(container.ingestion.run_stream_job(job_name))
                for job_name in stream_job_names
            ]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in pending:
                task.cancel()
            for task in done:
                exception = task.exception()
                if exception is not None:
                    raise exception
    finally:
        await container.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
