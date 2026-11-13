from __future__ import annotations

import json
import logging
import sys
from typing import Any


logger = logging.getLogger("pipeline_builder")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def emit(event: str, pipeline_id: str, **attributes: Any) -> None:
    """Emit structured logs suitable for collection by a container platform."""
    logger.info(json.dumps({"event": event, "pipeline_id": pipeline_id, **attributes}, default=str))
