from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, desc
from sqlalchemy.dialects.mysql import insert
from typing import List, Optional
from datetime import datetime
from src.models.kline import Kline, Symbol, Period

class KlineCRUD:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_symbol(self, symbol_name: str) -> Optional[Symbol]:
        result = await self.session.execute(
            select(Symbol).where(Symbol.name == symbol_name)
        )
        return result.scalar_one_or_none()

    async def create_symbol(self, name: str, base_asset: str, quote_asset: str) -> Symbol:
        symbol = Symbol(
            name=name,
            base_asset=base_asset,
            quote_asset=quote_asset
        )
        self.session.add(symbol)
        return symbol

    async def get_or_create_symbol(self, name: str, base_asset: str, quote_asset: str) -> Symbol:
        symbol = await self.get_symbol(name)
        if not symbol:
            symbol = await self.create_symbol(name, base_asset, quote_asset)
        return symbol

    async def bulk_insert_klines(self, klines_data: List[dict]) -> None:
        """批量插入K线数据"""
        if not klines_data:
            return
            
        # 使用 insert() 而不是 prefix_with
        stmt = insert(Kline)
        await self.session.execute(stmt, klines_data)

    async def get_latest_kline(self, symbol_id: int, period_id: str) -> Optional[Kline]:
        """获取最新的K线数据"""
        result = await self.session.execute(
            select(Kline)
            .where(
                and_(
                    Kline.symbol_id == symbol_id,
                    Kline.period_id == period_id
                )
            )
            .order_by(desc(Kline.open_time))
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_klines(
        self,
        symbol_id: int,
        period_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Kline]:
        """获取指定时间范围的K线数据"""
        result = await self.session.execute(
            select(Kline)
            .where(
                and_(
                    Kline.symbol_id == symbol_id,
                    Kline.period_id == period_id,
                    Kline.open_time >= start_time,
                    Kline.open_time <= end_time
                )
            )
            .order_by(Kline.open_time)
        )
        return result.scalars().all()

    async def get_klines_by_symbol_id(self, symbol_id: int) -> List[Kline]:
        """获取指定 symbol_id 的所有K线数据"""
        stmt = select(Kline).where(Kline.symbol_id == symbol_id)
        result = await self.session.execute(stmt)
        return result.scalars().all()
