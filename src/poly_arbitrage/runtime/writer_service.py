from __future__ import annotations

import asyncio

from poly_arbitrage.runtime.bootstrap import build_runtime


async def _run() -> None:
    runtime = build_runtime()
    try:
        while True:
            processed = await runtime.writer.persist_pending()
            if processed == 0:
                await asyncio.sleep(1)
    finally:
        await runtime.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
