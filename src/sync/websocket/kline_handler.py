import logging
import json
from datetime import datetime
from typing import Dict, Any
from src.database import get_redis
from src.config import SYNC_CONFIG, REDIS_CONFIG
from src.sync.websocket.ws_manager import BinanceWebsocketManager

logger = logging.getLogger(__name__)

class KlineWebsocketHandler:
    def __init__(self, symbol: str, interval: str):
        self.symbol = symbol.lower()  # Binance要求小写
        self.interval = interval
        self.stream_name = f"{self.symbol}@kline_{interval}"
        self.ws_manager = BinanceWebsocketManager()
        self.max_length = SYNC_CONFIG['KLINE']['MAX_LENGTH']

    async def _process_kline_message(self, message: Dict[str, Any]) -> None:
        """
        处理K线WebSocket消息
        消息格式：
        {
            "e": "kline",     // 事件类型
            "E": 1638747660000,   // 事件时间
            "s": "BTCUSDT",    // 交易对
            "k": {
                "t": 1638747660000, // K线开始时间
                "T": 1638747719999, // K线结束时间
                "s": "BTCUSDT",     // 交易对
                "i": "1m",          // 间隔
                "f": 100,           // 第一笔成交ID
                "L": 200,           // 最后一笔成交ID
                "o": "0.0010",      // 开盘价
                "c": "0.0020",      // 收盘价
                "h": "0.0025",      // 最高价
                "l": "0.0015",      // 最低价
                "v": "1000",        // 成交量
                "n": 100,           // 成交笔数
                "x": false,         // 是否完结
                "q": "1.0000",      // 成交额
                "V": "500",         // 主动买入成交量
                "Q": "0.500",       // 主动买入成交额
                "B": "123456"       // 忽略
            }
        }
        """
        try:
            k = message['k']
            
            # 只处理已完结的K线
            if not k['x']:
                return

            kline_data = {
                "symbol": k['s'],
                "interval": k['i'],
                "open_time": datetime.fromtimestamp(k['t'] / 1000),
                "close_time": datetime.fromtimestamp(k['T'] / 1000),
                "open": float(k['o']),
                "high": float(k['h']),
                "low": float(k['l']),
                "close": float(k['c']),
                "volume": float(k['v']),
                "quote_volume": float(k['q']),
                "trades": int(k['n']),
                "taker_buy_volume": float(k['V']),
                "taker_buy_quote_volume": float(k['Q'])
            }

            # 发布到Redis流
            await self._publish_to_redis(kline_data)
            
            logger.debug(f"Processed kline for {self.symbol} {self.interval}: {k['t']}")

        except Exception as e:
            logger.error(f"Error processing kline message: {str(e)}")

    async def _publish_to_redis(self, kline_data: Dict[str, Any]) -> None:
        """
        将K线数据发布到Redis流
        """
        try:
            redis = await get_redis()
            stream_key = f"market:kline:{self.symbol}:{self.interval}"
            
            # 使用时间戳作为消息ID
            timestamp = int(kline_data['open_time'].timestamp() * 1000)
            
            # 转换datetime为时间戳，因为Redis不能直接存储datetime对象
            kline_data['open_time'] = timestamp
            kline_data['close_time'] = int(kline_data['close_time'].timestamp() * 1000)
            
            await redis.xadd(
                stream_key,
                kline_data,
                id=f"{timestamp}-0",
                maxlen=self.max_length
            )
            
            logger.debug(f"Published kline to Redis: {stream_key}")

        except Exception as e:
            logger.error(f"Error publishing to Redis: {str(e)}")

    async def start(self) -> None:
        """
        启动K线WebSocket处理
        """
        try:
            # 添加消息处理回调
            self.ws_manager.add_callback(self.stream_name, self._process_kline_message)
            
            # 启动WebSocket管理器
            await self.ws_manager.start()
            
            # 连接到K线stream
            await self.ws_manager.connect([self.stream_name])
            
            logger.info(f"Started kline websocket handler for {self.symbol} {self.interval}")
            
        except Exception as e:
            logger.error(f"Error starting kline websocket handler: {str(e)}")
            raise

    async def stop(self) -> None:
        """
        停止K线WebSocket处理
        """
        await self.ws_manager.stop()
        logger.info(f"Stopped kline websocket handler for {self.symbol} {self.interval}")


# 使用示例
if __name__ == "__main__":
    import asyncio
    
    async def main():
        # 创建处理器实例
        handler = KlineWebsocketHandler("BTCUSDT", "1m")
        
        try:
            # 启动处理器
            await handler.start()
            
            # 保持运行
            while True:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            # 优雅退出
            await handler.stop()
            
        except Exception as e:
            logger.error(f"Error in main: {str(e)}")
            await handler.stop()

    # 设置日志级别
    logging.basicConfig(level=logging.INFO)
    
    # 运行
    asyncio.run(main()) 