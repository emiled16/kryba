from __future__ import annotations

from poly_arbitrage.config import Settings
from poly_arbitrage.storage.db import create_engine_and_session_factory
from poly_arbitrage.storage.models import Base


def main() -> None:
    settings = Settings.from_env()
    engine, _ = create_engine_and_session_factory(settings.database_url)
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    main()
