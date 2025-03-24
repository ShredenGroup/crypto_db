import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from src.database import engine, Base
from src.sync.kline_sync import BinanceDataSync, start_kline_sync_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def start_all_sync_tasks():
    """启动所有同步任务"""
    try:
        # 首先创建所有数据库表
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")

        # 创建所有同步任务
        tasks = [
            # K线数据同步 - BTCUSDT 1分钟
            start_kline_sync_scheduler(
                engine=engine,
                symbol="BTCUSDT",
                interval="1m"
            ),
            # 这里可以添加更多的同步任务，每个任务使用独立的会话
            # start_kline_sync_scheduler(
            #     engine=engine,
            #     symbol="ETHUSDT",
            #     interval="5m"
            # ),
        ]
        
        # 并发运行所有同步任务
        await asyncio.gather(*tasks)
            
    except Exception as e:
        logger.error(f"Error in sync tasks: {str(e)}")
        raise
    finally:
        # 确保在程序结束时关闭引擎
        await engine.dispose()

if __name__ == "__main__":
    try:
        logger.info("Starting sync service...")
        asyncio.run(start_all_sync_tasks())
    except KeyboardInterrupt:
        logger.info("Sync service stopped by user")
    except Exception as e:
        logger.error(f"Sync service error: {str(e)}")
