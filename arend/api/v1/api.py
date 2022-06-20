import logging

from arend.api.v1.endpoints.backend import router as backend
from arend.api.v1.endpoints.broker import router as broker
from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter()
router.prefix = "/v1"
router.include_router(router=backend, tags=["Backend Tasks"])
router.include_router(router=broker, tags=["Broker Tasks"])
# more routers to be added here...
