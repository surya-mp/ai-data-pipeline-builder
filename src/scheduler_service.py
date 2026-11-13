from __future__ import annotations

import argparse
import asyncio
from datetime import datetime

from src.execution.cron import cron_matches
from src.models import ReviewStatus
from src.service import PipelineService


async def serve(poll_seconds: int) -> None:
    service = PipelineService()
    dispatched: set[tuple[str, str]] = set()
    while True:
        now = datetime.now()
        minute_key = now.strftime("%Y%m%d%H%M")
        for item in service.list_pipelines():
            definition = service.load_definition(str(item["id"]))
            try:
                due = bool(definition.schedule and cron_matches(definition.schedule, now))
            except ValueError:
                # Legacy invalid schedules are ignored until explicitly repaired through the validated API.
                due = False
            if due and definition.semantic_review.status == ReviewStatus.APPROVED:
                key = (definition.id, minute_key)
                if key not in dispatched:
                    dispatched.add(key)
                    try:
                        await service.run_scheduled(definition.id)
                    except Exception:
                        # The executor records a durable failure; keep the scheduler alive for later runs.
                        pass
        dispatched = {key for key in dispatched if key[1] == minute_key}
        await asyncio.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run saved approved pipeline schedules.")
    parser.add_argument("--poll-seconds", type=int, default=15)
    args = parser.parse_args()
    asyncio.run(serve(args.poll_seconds))
