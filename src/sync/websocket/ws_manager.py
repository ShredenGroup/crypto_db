import asyncio
import json
import logging
import websockets
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
from src.config import BINANCE_CONFIG
from src.database import get_redis

logger = logging.getLogger(__name__)

class BinanceWebsocketManager:
    def __init__(self):
        self.ws_url = BINANCE_CONFIG['WS_URL']
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.is_running = False
        self.ping_interval = 180  # 3分钟发送一次ping
        self.pong_timeout = 600   # 10分钟没收到pong就断开
        self.last_pong: Dict[str, float] = {}

    async def connect(self, streams: List[str]) -> None:
        """
        连接到WebSocket streams
        """
        stream_path = '/stream?streams=' + '/'.join(streams) if len(streams) > 1 else f'/ws/{streams[0]}'
        url = f"{self.ws_url}{stream_path}"

        try:
            connection = await websockets.connect(url)
            connection_id = f"{'/'.join(streams)}"
            self.connections[connection_id] = connection
            self.last_pong[connection_id] = datetime.now().timestamp()
            
            # 启动心跳检测
            asyncio.create_task(self._heartbeat(connection_id))
            # 启动消息处理
            asyncio.create_task(self._handle_messages(connection_id))
            
            logger.info(f"Connected to streams: {streams}")
            return connection_id
        except Exception as e:
            logger.error(f"Failed to connect to streams {streams}: {str(e)}")
            raise

    async def _heartbeat(self, connection_id: str) -> None:
        """
        维护WebSocket心跳
        """
        while self.is_running and connection_id in self.connections:
            try:
                connection = self.connections[connection_id]
                await connection.ping()
                await asyncio.sleep(self.ping_interval)
                
                # 检查是否超时
                if datetime.now().timestamp() - self.last_pong[connection_id] > self.pong_timeout:
                    logger.warning(f"Connection {connection_id} pong timeout")
                    await self._handle_disconnect(connection_id)
                    break
            except Exception as e:
                logger.error(f"Heartbeat error for {connection_id}: {str(e)}")
                await self._handle_disconnect(connection_id)
                break

    async def _handle_messages(self, connection_id: str) -> None:
        """
        处理WebSocket消息
        """
        connection = self.connections[connection_id]
        
        while self.is_running and connection_id in self.connections:
            try:
                message = await connection.recv()
                if not message:
                    continue

                if isinstance(message, bytes):
                    # 处理pong消息
                    self.last_pong[connection_id] = datetime.now().timestamp()
                    continue

                # 处理JSON消息
                data = json.loads(message)
                await self._process_message(connection_id, data)

            except websockets.ConnectionClosed:
                logger.warning(f"Connection {connection_id} closed")
                await self._handle_disconnect(connection_id)
                break
            except Exception as e:
                logger.error(f"Error processing message for {connection_id}: {str(e)}")
                continue

    async def _process_message(self, connection_id: str, message: Dict[str, Any]) -> None:
        """
        处理接收到的消息
        """
        try:
            # 对于组合streams，消息格式为 {"stream": "streamName", "data": {...}}
            if "stream" in message:
                stream_name = message["stream"]
                data = message["data"]
            else:
                # 单个stream的消息直接是数据
                stream_name = connection_id
                data = message

            # 调用相应的回调函数
            if stream_name in self.callbacks:
                for callback in self.callbacks[stream_name]:
                    try:
                        await callback(data)
                    except Exception as e:
                        logger.error(f"Callback error for {stream_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    async def _handle_disconnect(self, connection_id: str) -> None:
        """
        处理连接断开
        """
        if connection_id in self.connections:
            connection = self.connections[connection_id]
            try:
                await connection.close()
            except:
                pass
            finally:
                del self.connections[connection_id]
                del self.last_pong[connection_id]

    def add_callback(self, stream: str, callback: Callable) -> None:
        """
        添加消息处理回调函数
        """
        if stream not in self.callbacks:
            self.callbacks[stream] = []
        self.callbacks[stream].append(callback)

    async def start(self) -> None:
        """
        启动WebSocket管理器
        """
        self.is_running = True

    async def stop(self) -> None:
        """
        停止WebSocket管理器
        """
        self.is_running = False
        for connection_id in list(self.connections.keys()):
            await self._handle_disconnect(connection_id)

# 使用示例
if __name__ == "__main__":
    async def kline_callback(message):
        print(f"Received kline: {message}")

    async def main():
        manager = BinanceWebsocketManager()
        # 添加K线回调
        manager.add_callback("btcusdt@kline_1m", kline_callback)
        
        await manager.start()
        # 连接到K线stream
        await manager.connect(["btcusdt@kline_1m"])
        
        try:
            # 保持运行
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await manager.stop()

    asyncio.run(main())
