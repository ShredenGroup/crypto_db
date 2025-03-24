import asyncio
import aiohttp
from datetime import datetime, timedelta
from src.models.kline import Kline, Symbol, Period
from sqlalchemy.ext.asyncio import AsyncSession
from src.curd.kline import KlineCRUD
import logging
from typing import Optional
from sqlalchemy.ext.asyncio import create_async_engine
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BinanceDataSync:
    def __init__(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
        base_asset: str = "BTC",
        quote_asset: str = "USDT",
        batch_size: int = 1500,
        db: AsyncSession = None
    ):
        self.base_url = "https://fapi.binance.com"
        self.symbol = symbol
        self.interval = interval
        self.batch_size = batch_size
        self.rate_limit = 6000
        self.current_weight = 0
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.db = db
    async def get_or_create_symbol_id(self) -> int:
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            "symbol": self.symbol,
            "interval": '1m',
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    async with self.db.begin():  # 使用事务上下文
                        # 先检查是否存在
                        kline_crud = KlineCRUD(self.db)
                        symbol = await kline_crud.get_symbol(self.symbol)
                        logger.info(f"Found symbol in database: {symbol}")
                        if not symbol:
                            # 不存在则创建
                            symbol = Symbol(
                                name=self.symbol,
                                base_asset=self.base_asset,
                                quote_asset=self.quote_asset
                            )
                            self.db.add(symbol)
                            await self.db.flush()  # 刷新以获取 ID
                            logger.info(f"Created new symbol: {self.symbol} with ID: {symbol.id}")
                        else:
                            logger.info(f"Found existing symbol: {self.symbol} with ID: {symbol.id}")
                        return symbol.id
                else:
                    logger.error(f"API request failed with status {response.status}")
                    raise Exception(f"API request failed with status {response.status}")

    async def get_klines(self, start_time: int, end_time: int) -> list:
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": self.batch_size,
            "startTime": start_time,
            "endTime": end_time
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    self.current_weight = int(response.headers.get('X-MBX-USED-WEIGHT-1m', 0))
                    return await response.json()
                else:
                    logger.error(f"API request failed with status {response.status}")
                    raise Exception(f"API request failed with status {response.status}")

    async def save_klines(self, db: AsyncSession, symbol_id: int, klines_data: list) -> None:
        try:
            klines_to_insert = [
                {
                    "symbol_id": symbol_id,
                    "period_id": self.interval,
                    "open_time": datetime.fromtimestamp(kline[0] / 1000),
                    "close_time": datetime.fromtimestamp(kline[6] / 1000),
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                    "quote_volume": float(kline[7]),
                    "trade_count": int(kline[8]),
                    "taker_buy_volume": float(kline[9]),
                    "taker_buy_quote_volume": float(kline[10])
                }
                for kline in klines_data
            ]
            
            kline_crud = KlineCRUD(db)
            # 直接执行事务
            await db.begin()
            try:
                logger.info(f"Inserting {len(klines_data)} klines...")
                await kline_crud.bulk_insert_klines(klines_to_insert)
                await db.commit()
                logger.info(f"Successfully saved {len(klines_data)} klines for {self.symbol}")
                
                # 立即验证
                result = await kline_crud.get_klines_by_symbol_id(symbol_id)
                logger.info(f"Verified: {len(result)} klines found in database")
                
            except Exception as e:
                await db.rollback()
                logger.error(f"Transaction rolled back: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"Failed to save klines: {str(e)}")
            raise

    async def sync_historical_data(
        self,
        db: AsyncSession,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> None:
        """
        同步历史K线数据
        
        Args:
            db: 数据库会话
            start_date: 开始时间，如果为None则使用当前时间前推24小时
            end_date: 结束时间，如果为None则使用当前时间
        """
        try:
            # 设置默认时间范围
            if end_date is None:
                end_date = datetime.now()
            if start_date is None:
                start_date = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

            # 获取或创建Symbol
            kline_crud = KlineCRUD(db)
            symbol = await kline_crud.get_or_create_symbol(
                name=self.symbol,
                base_asset=self.base_asset,
                quote_asset=self.quote_asset
            )

            current_time = start_date
            while current_time < end_date:
                if self.current_weight > 5900:
                    logger.info("Rate limit approaching, waiting...")
                    await asyncio.sleep(60)
                    self.current_weight = 0

                batch_end = min(
                    current_time + timedelta(minutes=self.batch_size),
                    end_date
                )
                
                try:
                    klines = await self.get_klines(
                        int(current_time.timestamp() * 1000),
                        int(batch_end.timestamp() * 1000)
                    )
                    
                    if klines:
                        await self.save_klines(db, symbol.id, klines)
                        logger.info(f"Synced data from {current_time} to {batch_end}")
                    
                    current_time = batch_end
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error syncing klines for {self.symbol} at {current_time}: {str(e)}")
                    await asyncio.sleep(1)
                    continue
                
        except Exception as e:
            logger.error(f"Failed to sync historical data: {str(e)}")
            raise

async def start_kline_sync_scheduler(
    engine: create_async_engine,  # 改为接收engine
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    base_asset: str = None,
    quote_asset: str = None,
    batch_size: int = 1500
):
    try:
        logger.info(f"Starting kline sync scheduler for {symbol} {interval}...")
        
        # 自动设置 base_asset 和 quote_asset
        if base_asset is None:
            base_asset = symbol[:-4]
        if quote_asset is None:
            quote_asset = symbol[-4:]
        
        syncer = BinanceDataSync(
            symbol=symbol,
            interval=interval,
            base_asset=base_asset,
            quote_asset=quote_asset,
            batch_size=batch_size
        )
        
        while True:
            try:
                # 每次循环创建新的会话
                async with AsyncSession(engine) as session:
                    async with session.begin():
                        await syncer.sync_historical_data(session)
                        logger.info(f"Completed kline sync cycle for {symbol} {interval}")
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Kline sync scheduler error for {symbol} {interval}: {str(e)}")
                await asyncio.sleep(60)
    except Exception as e:
        logger.error(f"Fatal error in sync scheduler: {str(e)}")
        raise

if __name__ == "__main__":
    import asyncio
    from src.database import async_session, engine  # 导入 engine
    
    async def main():
        async with async_session() as session:  # 使用 async_session 工厂函数
            bt = BinanceDataSync(db=session, symbol="ETHUSDT", interval="1m", base_asset="BTC", quote_asset="USDT", batch_size=1500)
            await bt.get_or_create_symbol_id()
    
    asyncio.run(main())