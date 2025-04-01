import asyncio
import logging
import signal
from typing import List, Dict, Any
from src.config import LOGGING_CONFIG
from src.sync.websocket.kline_handler import KlineWebsocketHandler
from src.sync.websocket.orderbook_handler import OrderBookWebsocketHandler
from src.sync.processors.kline_processor import KlineProcessor
from src.sync.rest.kline_fetcher import BinanceKlineFetcher
from datetime import datetime, timedelta

# 配置日志
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

class MarketDataSyncer:
    def __init__(self, symbols: List[str], intervals: List[str], 
                 sync_days: int = 1, batch_hours: int = 4):
        self.symbols = symbols
        self.intervals = intervals
        self.sync_days = sync_days  # 同步天数
        self.batch_hours = batch_hours  # 每批次处理的小时数
        self.handlers: Dict[str, Any] = {}
        self.processors: Dict[str, Any] = {}
        self.is_running = False

    async def init_handlers(self):
        """
        初始化所有处理器
        """
        # 初始化K线处理器
        for symbol in self.symbols:
            for interval in self.intervals:
                # WebSocket实时K线处理器
                kline_handler = KlineWebsocketHandler(symbol, interval)
                self.handlers[f"kline_{symbol}_{interval}"] = kline_handler
                
                # K线数据处理器（Redis到MySQL）
                kline_processor = KlineProcessor(symbol, interval)
                self.processors[f"kline_{symbol}_{interval}"] = kline_processor
            
            # 订单簿处理器
            orderbook_handler = OrderBookWebsocketHandler(symbol)
            self.handlers[f"orderbook_{symbol}"] = orderbook_handler

    async def sync_historical_data(self):
        """
        同步历史数据，支持分批处理
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=self.sync_days)
        
        for symbol in self.symbols:
            for interval in self.intervals:
                try:
                    fetcher = BinanceKlineFetcher(
                        symbol, 
                        interval,
                        batch_hours=self.batch_hours
                    )
                    logger.info(f"Syncing historical data for {symbol} {interval} "
                              f"from {start_time} to {end_time}")
                    await fetcher.sync_historical_data(start_time, end_time)
                except Exception as e:
                    logger.error(f"Error syncing {symbol} {interval}: {str(e)}")
                    continue

    async def start(self):
        """
        启动所有服务
        """
        self.is_running = True
        
        # 初始化处理器
        await self.init_handlers()
        
        # 同步历史数据
        await self.sync_historical_data()
        
        # 启动所有处理器
        for handler in self.handlers.values():
            await handler.start()
        
        for processor in self.processors.values():
            await processor.start_processing()
        
        logger.info("All services started successfully")

    async def stop(self):
        """
        停止所有服务
        """
        self.is_running = False
        
        # 停止所有处理器
        for handler in self.handlers.values():
            await handler.stop()
        
        logger.info("All services stopped successfully")

async def main():
    # 配置要同步的交易对和时间间隔
    symbols = ["BTCUSDT", "ETHUSDT"]
    intervals = ["1m", "5m", "15m"]
    
    # 创建同步器实例
    syncer = MarketDataSyncer(symbols, intervals)
    
    # 注册信号处理
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(syncer.stop()))
    
    try:
        # 启动服务
        await syncer.start()
        
        # 保持运行
        while syncer.is_running:
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
    finally:
        await syncer.stop()

if __name__ == "__main__":
    asyncio.run(main())
