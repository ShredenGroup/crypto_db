import aiohttp
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from src.database import get_redis
from src.config import BINANCE_CONFIG, SYNC_CONFIG

logger = logging.getLogger(__name__)

class OrderBookSnapshotFetcher:
    def __init__(self, symbol: str = "BTCUSDT"):
        self.base_url = BINANCE_CONFIG['BASE_URL']
        self.symbol = symbol
        self.depth_limit = SYNC_CONFIG['ORDERBOOK']['DEPTH_LEVEL']

    async def fetch_snapshot(self) -> Optional[Dict[str, Any]]:
        """
        获取订单簿快照
        """
        url = f"{self.base_url}/fapi/v1/depth"
        params = {
            "symbol": self.symbol,
            "limit": self.depth_limit
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._process_snapshot(data)
                    else:
                        logger.error(f"Failed to fetch snapshot: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Error fetching snapshot: {str(e)}")
            return None

    def _process_snapshot(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理快照数据
        """
        return {
            "symbol": self.symbol,
            "lastUpdateId": data["lastUpdateId"],
            "timestamp": datetime.now(),
            "bids": [[float(price), float(qty)] for price, qty in data["bids"]],
            "asks": [[float(price), float(qty)] for price, qty in data["asks"]]
        }

    async def save_snapshot(self, snapshot: Dict[str, Any]):
        """
        保存快照到Redis
        """
        if not snapshot:
            return

        redis = await get_redis()
        snapshot_key = f"orderbook:snapshot:{self.symbol}"
        
        try:
            # 保存快照数据
            await redis.set(
                snapshot_key,
                {
                    "lastUpdateId": snapshot["lastUpdateId"],
                    "timestamp": int(snapshot["timestamp"].timestamp() * 1000),
                    "bids": snapshot["bids"],
                    "asks": snapshot["asks"]
                }
            )
            logger.info(f"Saved snapshot for {self.symbol} with lastUpdateId: {snapshot['lastUpdateId']}")
        except Exception as e:
            logger.error(f"Error saving snapshot: {str(e)}")

    async def fetch_and_save(self):
        """
        获取并保存快照
        """
        snapshot = await self.fetch_snapshot()
        if snapshot:
            await self.save_snapshot(snapshot)
            return snapshot
        return None

async def get_latest_snapshot(symbol: str) -> Optional[Dict[str, Any]]:
    """
    获取最新的订单簿快照
    """
    redis = await get_redis()
    snapshot_key = f"orderbook:snapshot:{symbol}"
    
    try:
        snapshot = await redis.get(snapshot_key)
        if snapshot:
            return {
                **snapshot,
                "timestamp": datetime.fromtimestamp(snapshot["timestamp"] / 1000)
            }
    except Exception as e:
        logger.error(f"Error getting snapshot from Redis: {str(e)}")
    
    return None

if __name__ == "__main__":
    import asyncio
    
    async def test_snapshot():
        fetcher = OrderBookSnapshotFetcher("BTCUSDT")
        snapshot = await fetcher.fetch_and_save()
        if snapshot:
            print(f"Successfully fetched and saved snapshot with lastUpdateId: {snapshot['lastUpdateId']}")
    
    asyncio.run(test_snapshot())
