from typing import List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from src.crud.base import CRUDBase
from src.models.orderbook import OrderBook

class CRUDOrderBook(CRUDBase[OrderBook]):
    async def get_latest_snapshot(
        self,
        db: AsyncSession,
        *,
        symbol: str
    ) -> Optional[OrderBook]:
        """
        获取最新的订单簿快照
        """
        query = (
            select(self.model)
            .filter(self.model.symbol == symbol)
            .order_by(self.model.timestamp.desc())
            .limit(1)
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

orderbook = CRUDOrderBook(OrderBook)
