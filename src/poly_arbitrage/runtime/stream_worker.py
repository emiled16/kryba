from __future__ import annotations

import asyncio

from poly_arbitrage.runtime.bootstrap import build_runtime


async def _run() -> None:
    runtime = build_runtime()
    try:
        stream_job_names = runtime.ingestion.list_stream_jobs()
        while True:
            if not stream_job_names:
                await asyncio.sleep(5)
                continue
            tasks = [
                asyncio.create_task(runtime.ingestion.run_stream_job(job_name))
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
        await runtime.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
