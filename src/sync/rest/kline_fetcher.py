import aiohttp
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from src.database import get_redis
from src.config import BINANCE_CONFIG, SYNC_CONFIG

logger = logging.getLogger(__name__)

class BinanceKlineFetcher:
    def __init__(self, symbol: str = "BTCUSDT", interval: str = "1m"):
        self.base_url = BINANCE_CONFIG['BASE_URL']
        self.symbol = symbol
        self.interval = interval
        self.batch_size = SYNC_CONFIG['KLINE']['BATCH_SIZE']
        self.semaphore = asyncio.Semaphore(SYNC_CONFIG['KLINE']['MAX_CONCURRENT_REQUESTS'])

    async def fetch_klines(self, start_time: int, end_time: int) -> List[Dict[str, Any]]:
        """
        获取指定时间范围的K线数据
        """
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": self.batch_size,
            "startTime": start_time,
            "endTime": end_time
        }

        async with self.semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            return self._process_kline_data(data)
                        else:
                            logger.error(f"API request failed with status {response.status}")
                            return []
            except Exception as e:
                logger.error(f"Error fetching klines: {str(e)}")
                return []

    def _process_kline_data(self, klines: List[List]) -> List[Dict[str, Any]]:
        """
        处理K线数据为标准格式
        """
        processed_klines = []
        for k in klines:
            kline_data = {
                "symbol": self.symbol,
                "interval": self.interval,
                "open_time": datetime.fromtimestamp(k[0] / 1000),
                "close_time": datetime.fromtimestamp(k[6] / 1000),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "quote_volume": float(k[7]),
                "trades": int(k[8]),
                "taker_buy_volume": float(k[9]),
                "taker_buy_quote_volume": float(k[10])
            }
            processed_klines.append(kline_data)
        return processed_klines

    async def publish_to_redis(self, klines: List[Dict[str, Any]]):
        """
        将K线数据发布到Redis
        """
        if not klines:
            return

        redis = await get_redis()
        stream_key = f"market:kline:{self.symbol}:{self.interval}"
        
        for kline in klines:
            try:
                # 使用时间戳作为消息ID
                timestamp = int(kline['open_time'].timestamp() * 1000)
                await redis.xadd(
                    stream_key,
                    kline,
                    id=f"{timestamp}-0",
                    maxlen=SYNC_CONFIG['KLINE']['MAX_LENGTH']
                )
            except Exception as e:
                logger.error(f"Error publishing to Redis: {str(e)}")

    async def sync_historical_data(self, start_time: datetime, end_time: datetime):
        """
        分批同步历史K线数据
        """
        current_time = int(start_time.timestamp() * 1000)
        end_timestamp = int(end_time.timestamp() * 1000)
        
        # 计算每批次的时间范围（例如：每次同步4小时的数据）
        batch_timeframe = 4 * 60 * 60 * 1000  # 4小时，单位：毫秒
        
        while current_time < end_timestamp:
            batch_end = min(current_time + batch_timeframe, end_timestamp)
            
            try:
                # 获取这个时间段的数据
                klines = await self.fetch_klines(current_time, batch_end)
                if klines:
                    # 分批发送到Redis
                    chunk_size = 100  # 每次发送100条数据
                    for i in range(0, len(klines), chunk_size):
                        chunk = klines[i:i + chunk_size]
                        await self.publish_to_redis(chunk)
                        logger.info(f"Synced {len(chunk)} klines from "
                                  f"{datetime.fromtimestamp(current_time/1000)} to "
                                  f"{datetime.fromtimestamp(batch_end/1000)}")
                        
                        # 添加小延迟，避免请求过快
                        await asyncio.sleep(0.1)
                
                current_time = batch_end
                # 每批次处理完后添加延迟，避免API限制
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error syncing historical data: {str(e)}")
                # 出错后等待更长时间再重试
                await asyncio.sleep(5)
                continue
