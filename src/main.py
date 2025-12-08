from __future__ import annotations
from typing import Optional

from fastapi import FastAPI
from src.routes import base,data, nlp
from .utils.metrics import setup_metrics
from .utils.client_deps_container import DependencyContainer

app = FastAPI()


# Setup Prometheus metrics
setup_metrics(app)


# store container on app.state
app.state.container: Optional[DependencyContainer] = None


@app.on_event("startup")
async def startup_span():
    app.state.container = await DependencyContainer.create()


@app.on_event("shutdown")
async def shutdown_span():
    if app.state.container is not None:
        await app.state.container.shutdown()

app.include_router(base.base_router)
app.include_router(data.data_router)
app.include_router(nlp.nlp_router)
