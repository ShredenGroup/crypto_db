import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_redis, async_session
from src.config import SYNC_CONFIG, REDIS_CONFIG
from src.crud.kline import kline as kline_crud
from src.models.kline import Kline

logger = logging.getLogger(__name__)

class KlineProcessor:
    def __init__(self, symbol: str, interval: str):
        self.symbol = symbol
        self.interval = interval
        self.stream_key = f"market:kline:{symbol}:{interval}"
        self.group_name = REDIS_CONFIG['STREAMS']['CONSUMER_GROUP']
        self.consumer_name = f"kline_processor_{symbol}_{interval}"
        self.batch_size = SYNC_CONFIG['KLINE']['CONSUMER_BATCH_SIZE']
        self.processing_interval = SYNC_CONFIG['KLINE']['PROCESSING_INTERVAL']

    async def init_consumer_group(self):
        """
        初始化消费者组
        """
        redis = await get_redis()
        try:
            # 创建消费者组，如果已存在则忽略
            await redis.xgroup_create(
                self.stream_key,
                self.group_name,
                id='0-0',  # 从头开始消费
                mkstream=True  # 如果stream不存在则创建
            )
        except Exception as e:
            logger.info(f"Consumer group might already exist: {e}")

    async def process_klines(self, klines_data: List[Dict[str, Any]], db: AsyncSession):
        """
        处理K线数据并保存到数据库
        """
        try:
            klines_to_save = []
            for kline in klines_data:
                kline_obj = {
                    "symbol": kline['symbol'],
                    "interval": kline['interval'],
                    "open_time": kline['open_time'],
                    "close_time": kline['close_time'],
                    "open_price": float(kline['open']),
                    "high_price": float(kline['high']),
                    "low_price": float(kline['low']),
                    "close_price": float(kline['close']),
                    "volume": float(kline['volume']),
                    "quote_volume": float(kline['quote_volume']),
                    "trade_count": int(kline['trades']),
                    "taker_buy_volume": float(kline['taker_buy_volume']),
                    "taker_buy_quote_volume": float(kline['taker_buy_quote_volume'])
                }
                klines_to_save.append(kline_obj)

            if klines_to_save:
                await kline_crud.bulk_create(db, objs_in=klines_to_save)
                logger.info(f"Saved {len(klines_to_save)} klines to database")

        except Exception as e:
            logger.error(f"Error processing klines: {str(e)}")
            raise

    async def start_processing(self):
        """
        开始处理K线数据
        """
        await self.init_consumer_group()
        last_id = '0-0'  # 从头开始读取

        while True:
            try:
                redis = await get_redis()
                async with async_session() as db:
                    # 读取新消息
                    messages = await redis.xreadgroup(
                        self.group_name,
                        self.consumer_name,
                        {self.stream_key: '>'},  # '>' 表示未被消费的消息
                        count=self.batch_size
                    )

                    if messages:
                        stream_messages = messages[0][1]  # 获取消息列表
                        klines_data = []
                        
                        for message_id, message_data in stream_messages:
                            try:
                                # 转换时间戳
                                message_data['open_time'] = datetime.fromtimestamp(
                                    int(message_data['open_time'].timestamp())
                                )
                                message_data['close_time'] = datetime.fromtimestamp(
                                    int(message_data['close_time'].timestamp())
                                )
                                klines_data.append(message_data)
                                last_id = message_id
                            except Exception as e:
                                logger.error(f"Error processing message {message_id}: {str(e)}")
                                continue

                        if klines_data:
                            await self.process_klines(klines_data, db)
                            # 确认消息已处理
                            await redis.xack(self.stream_key, self.group_name, *[msg[0] for msg in stream_messages])

            except Exception as e:
                logger.error(f"Error in processing loop: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待一段时间再重试
                continue

            await asyncio.sleep(self.processing_interval)

async def start_kline_processor(symbol: str, interval: str):
    """
    启动K线处理器
    """
    processor = KlineProcessor(symbol, interval)
    await processor.start_processing()

if __name__ == "__main__":
    import asyncio
    asyncio.run(start_kline_processor("BTCUSDT", "1m"))
