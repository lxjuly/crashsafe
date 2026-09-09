"""HTTP boundary for creating workflow runs and reading durable run views.

FastAPI validates submitted definitions before this module delegates persistence
to ``SQLiteStorage``. Workers execute independently; API handlers never run steps.
"""

from __future__ import annotations

import os
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, status

from crashsafe.config import DEFAULT_API_HOST, DEFAULT_API_PORT, Settings
from crashsafe.models import (
    WorkflowDefinition,
    WorkflowRun,
    WorkflowRunEvent,
    WorkflowRunTimeline,
)
from crashsafe.observability import build_timeline
from crashsafe.storage import SQLiteStorage, WorkflowRunNotFoundError


def create_app(storage: Optional[SQLiteStorage] = None) -> FastAPI:
    """Build the API, optionally around an injected store for deterministic tests."""
    settings = Settings.from_env()
    database = storage or SQLiteStorage(settings.engine_db, settings.tool_key)
    app = FastAPI(title="Crashsafe workflow run API", version="0.1.0")

    def get_storage() -> SQLiteStorage:
        return database

    @app.post("/workflow_runs", response_model=WorkflowRun, status_code=status.HTTP_201_CREATED)
    def create_workflow_run(
        request: WorkflowDefinition,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowRun:
        # A 201 means the definition snapshot, creation event, and scheduling
        # projection committed together; the source JSON is not needed to resume.
        return store.create_workflow_run(request)

    @app.get("/workflow_runs/{run_id}", response_model=WorkflowRun)
    def get_workflow_run(
        run_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowRun:
        try:
            return store.get_workflow_run(run_id)
        except WorkflowRunNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow run not found"
            ) from exc

    @app.get("/workflow_runs", response_model=list[WorkflowRun])
    def list_workflow_runs(
        limit: int = Query(default=100, ge=1, le=500),
        store: SQLiteStorage = Depends(get_storage),
    ) -> list[WorkflowRun]:
        return store.list_workflow_runs(limit)

    @app.get("/workflow_runs/{run_id}/events", response_model=list[WorkflowRunEvent])
    def list_workflow_run_events(
        run_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> list[WorkflowRunEvent]:
        try:
            return store.list_events(run_id)
        except WorkflowRunNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow run not found"
            ) from exc

    @app.get("/workflow_runs/{run_id}/timeline", response_model=WorkflowRunTimeline)
    def workflow_run_timeline(
        run_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowRunTimeline:
        try:
            # Observability is derived from authoritative history, not a second
            # independently updated audit or metrics store.
            return build_timeline(store.list_events(run_id))
        except WorkflowRunNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow run not found"
            ) from exc

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def run() -> None:
    port = int(os.getenv("CRASHSAFE_API_PORT", str(DEFAULT_API_PORT)))
    uvicorn.run(create_app(), host=DEFAULT_API_HOST, port=port, log_level="info")


if __name__ == "__main__":
    run()
