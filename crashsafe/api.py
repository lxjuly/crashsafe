from __future__ import annotations

import os
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, status

from crashsafe.config import DEFAULT_API_HOST, DEFAULT_API_PORT, Settings
from crashsafe.models import (
    WorkflowAudit,
    WorkflowCreate,
    WorkflowEventRecord,
    WorkflowRecord,
    WorkflowTimeline,
)
from crashsafe.observability import build_timeline
from crashsafe.storage import SQLiteStorage, WorkflowNotFoundError


def create_app(storage: Optional[SQLiteStorage] = None) -> FastAPI:
    settings = Settings.from_env()
    database = storage or SQLiteStorage(settings.engine_db, settings.tool_key)
    app = FastAPI(title="Crashsafe workflow API", version="0.1.0")

    def get_storage() -> SQLiteStorage:
        return database

    @app.post("/workflows", response_model=WorkflowRecord, status_code=status.HTTP_201_CREATED)
    def create_workflow(
        request: WorkflowCreate,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowRecord:
        return store.create_workflow(request)

    @app.get("/workflows/{workflow_id}", response_model=WorkflowRecord)
    def get_workflow(
        workflow_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowRecord:
        try:
            return store.get_workflow(workflow_id)
        except WorkflowNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found"
            ) from exc

    @app.get("/workflows", response_model=list[WorkflowRecord])
    def list_workflows(
        limit: int = Query(default=100, ge=1, le=500),
        store: SQLiteStorage = Depends(get_storage),
    ) -> list[WorkflowRecord]:
        return store.list_workflows(limit)

    @app.get("/workflows/{workflow_id}/events", response_model=list[WorkflowEventRecord])
    def list_workflow_events(
        workflow_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> list[WorkflowEventRecord]:
        try:
            return store.list_events(workflow_id)
        except WorkflowNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found"
            ) from exc

    @app.get("/workflows/{workflow_id}/audit", response_model=WorkflowAudit)
    def audit_workflow(
        workflow_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowAudit:
        try:
            return store.audit_workflow(workflow_id)
        except WorkflowNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found"
            ) from exc

    @app.get("/workflows/{workflow_id}/timeline", response_model=WorkflowTimeline)
    def workflow_timeline(
        workflow_id: str,
        store: SQLiteStorage = Depends(get_storage),
    ) -> WorkflowTimeline:
        try:
            return build_timeline(
                store.list_events(workflow_id), store.audit_workflow(workflow_id)
            )
        except WorkflowNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found"
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
