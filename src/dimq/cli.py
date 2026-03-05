from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from dimq.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="DIMQ - Distributed In-Memory Queue")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Orchestrator
    orch_parser = subparsers.add_parser("orchestrator", help="Run the orchestrator")
    orch_parser.add_argument("--config", type=Path, required=True)

    # Worker
    worker_parser = subparsers.add_parser("worker", help="Run a worker")
    worker_parser.add_argument("--config", type=Path, required=True)
    worker_parser.add_argument("--endpoint", type=str, default=None,
                                help="Override orchestrator endpoint")

    args = parser.parse_args()
    config = load_config(args.config)

    if args.command == "orchestrator":
        from dimq.orchestrator import Orchestrator
        orch = Orchestrator(config)
        asyncio.run(orch.run())

    elif args.command == "worker":
        if args.endpoint:
            config.endpoint = args.endpoint
        from dimq.worker import Worker
        worker = Worker(config)
        asyncio.run(worker.run())


if __name__ == "__main__":
    main()
