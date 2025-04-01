import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
from src.database import get_redis
from src.config import SYNC_CONFIG
from src.sync.websocket.ws_manager import BinanceWebsocketManager
from src.sync.rest.snapshot_fetcher import OrderBookSnapshotFetcher, get_latest_snapshot

logger = logging.getLogger(__name__)

class OrderBookWebsocketHandler:
    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self.stream_name = f"{self.symbol}@depth@100ms"  # 使用100ms更新速度
        self.ws_manager = BinanceWebsocketManager()
        self.snapshot_fetcher = OrderBookSnapshotFetcher(symbol)
        self.max_length = SYNC_CONFIG['ORDERBOOK']['MAX_LENGTH']
        
        # 本地订单簿状态
        self.bids: Dict[float, float] = {}  # price -> quantity
        self.asks: Dict[float, float] = {}
        self.last_update_id: Optional[int] = None
        self.processing_buffer: List[Dict] = []  # 缓存未处理的消息

    def _initialize_orderbook(self, snapshot: Dict[str, Any]) -> None:
        """
        使用快照初始化订单簿
        """
        self.bids.clear()
        self.asks.clear()
        
        # 初始化买单和卖单
        for price, qty in snapshot['bids']:
            if float(qty) > 0:
                self.bids[float(price)] = float(qty)
        
        for price, qty in snapshot['asks']:
            if float(qty) > 0:
                self.asks[float(price)] = float(qty)
        
        self.last_update_id = snapshot['lastUpdateId']

    def _process_depth_update(self, data: Dict[str, Any]) -> bool:
        """
        处理深度更新
        返回是否成功处理
        """
        # 验证更新ID的连续性
        if self.last_update_id is None:
            return False
            
        if data['U'] <= self.last_update_id + 1 <= data['u']:
            # 更新买单
            for price, qty in data['b']:
                price = float(price)
                qty = float(qty)
                if qty == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = qty
            
            # 更新卖单
            for price, qty in data['a']:
                price = float(price)
                qty = float(qty)
                if qty == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = qty
            
            self.last_update_id = data['u']
            return True
        return False

    def _get_current_orderbook(self) -> Dict[str, Any]:
        """
        获取当前订单簿状态
        """
        return {
            "symbol": self.symbol,
            "lastUpdateId": self.last_update_id,
            "timestamp": datetime.now(),
            "bids": [[price, qty] for price, qty in sorted(self.bids.items(), reverse=True)],
            "asks": [[price, qty] for price, qty in sorted(self.asks.items())]
        }

    async def _publish_to_redis(self, orderbook: Dict[str, Any]) -> None:
        """
        将订单簿数据发布到Redis
        """
        try:
            redis = await get_redis()
            stream_key = f"market:orderbook:{self.symbol}"
            
            # 转换时间戳
            data = {
                **orderbook,
                "timestamp": int(orderbook["timestamp"].timestamp() * 1000)
            }
            
            # 发布到Redis流
            await redis.xadd(
                stream_key,
                data,
                maxlen=self.max_length
            )
            
            # 同时更新最新状态
            await redis.set(f"orderbook:latest:{self.symbol}", data)
            
            logger.debug(f"Published orderbook to Redis: {stream_key}")

        except Exception as e:
            logger.error(f"Error publishing to Redis: {str(e)}")

    async def _process_depth_message(self, message: Dict[str, Any]) -> None:
        """
        处理深度WebSocket消息
        """
        try:
            # 如果没有初始化订单簿，获取快照
            if self.last_update_id is None:
                snapshot = await self.snapshot_fetcher.fetch_snapshot()
                if snapshot:
                    self._initialize_orderbook(snapshot)
                    logger.info(f"Initialized orderbook for {self.symbol}")
                else:
                    logger.error("Failed to get snapshot for initialization")
                    return

            # 处理深度更新
            if self._process_depth_update(message):
                # 发布更新后的订单簿
                orderbook = self._get_current_orderbook()
                await self._publish_to_redis(orderbook)
            else:
                logger.warning(f"Out of sync, reinitializing orderbook for {self.symbol}")
                # 重新初始化订单簿
                snapshot = await self.snapshot_fetcher.fetch_snapshot()
                if snapshot:
                    self._initialize_orderbook(snapshot)

        except Exception as e:
            logger.error(f"Error processing depth message: {str(e)}")

    async def start(self) -> None:
        """
        启动订单簿WebSocket处理
        """
        try:
            # 获取初始快照
            snapshot = await self.snapshot_fetcher.fetch_snapshot()
            if snapshot:
                self._initialize_orderbook(snapshot)
                logger.info(f"Initial snapshot loaded for {self.symbol}")
            else:
                raise Exception("Failed to get initial snapshot")

            # 添加消息处理回调
            self.ws_manager.add_callback(self.stream_name, self._process_depth_message)
            
            # 启动WebSocket管理器
            await self.ws_manager.start()
            
            # 连接到深度stream
            await self.ws_manager.connect([self.stream_name])
            
            logger.info(f"Started orderbook websocket handler for {self.symbol}")
            
        except Exception as e:
            logger.error(f"Error starting orderbook websocket handler: {str(e)}")
            raise

    async def stop(self) -> None:
        """
        停止订单簿WebSocket处理
        """
        await self.ws_manager.stop()
        logger.info(f"Stopped orderbook websocket handler for {self.symbol}")


# 使用示例
if __name__ == "__main__":
    import asyncio
    
    async def main():
        # 创建处理器实例
        handler = OrderBookWebsocketHandler("BTCUSDT")
        
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
