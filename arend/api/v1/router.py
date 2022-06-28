import logging

from arend.api.v1.endpoints.backend import router as backend
from arend.api.v1.endpoints.broker import router as broker
from fastapi import APIRouter

logger = logging.getLogger(__name__)

arend_router = APIRouter()
arend_router.prefix = "/v1"
arend_router.include_router(router=backend, tags=["Backend Arend"])
arend_router.include_router(router=broker, tags=["Broker Arend"])
# more routers to be added here...
