from typing import List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from src.crud.base import CRUDBase
from src.models.kline import Kline

class CRUDKline(CRUDBase[Kline]):
    async def get_by_timerange(
        self,
        db: AsyncSession,
        *,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str
    ) -> List[Kline]:
        """
        获取指定时间范围的K线数据
        """
        query = (
            select(self.model)
            .filter(
                and_(
                    self.model.symbol == symbol,
                    self.model.interval == interval,
                    self.model.start_time >= start_time,
                    self.model.start_time <= end_time
                )
            )
            .order_by(self.model.start_time)
        )
        result = await db.execute(query)
        return result.scalars().all()

    async def get_latest(
        self,
        db: AsyncSession,
        *,
        symbol: str,
        interval: str
    ) -> Optional[Kline]:
        """
        获取最新的K线数据
        """
        query = (
            select(self.model)
            .filter(
                and_(
                    self.model.symbol == symbol,
                    self.model.interval == interval
                )
            )
            .order_by(self.model.start_time.desc())
            .limit(1)
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

kline = CRUDKline(Kline)
