from __future__ import annotations

import asyncio

from poly_arbitrage.runtime.bootstrap import build_container


async def _run() -> None:
    container = build_container()
    try:
        while True:
            processed = await container.writer.persist_pending()
            if processed == 0:
                await asyncio.sleep(1)
    finally:
        await container.aclose()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
