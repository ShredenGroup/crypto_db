from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import aioredis
from redis import asyncio as aioredis
import dotenv
import os
import asyncio
from typing import Optional

dotenv.load_dotenv()

# MySQL 配置
DATABASE_URL = f"mysql+aiomysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Redis 配置
REDIS_URL = f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', 6379)}"

# SQLAlchemy 设置
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# Redis 客户端
redis_client: Optional[aioredis.Redis] = None

async def init_redis() -> aioredis.Redis:
    """
    初始化Redis连接
    """
    global redis_client
    if redis_client is None:
        redis_client = await aioredis.from_url(
            REDIS_URL,
            db=int(os.getenv('REDIS_DB', 0)),
            password=os.getenv('REDIS_PASSWORD'),
            decode_responses=True,  # 自动解码响应
            encoding='utf-8'
        )
    return redis_client

async def get_redis() -> aioredis.Redis:
    """
    获取Redis客户端实例
    """
    if redis_client is None:
        await init_redis()
    return redis_client

async def close_redis():
    """
    关闭Redis连接
    """
    global redis_client
    if redis_client is not None:
        await redis_client.close()
        redis_client = None

async def test_db_connection():
    """
    测试数据库连接
    """
    try:
        async with engine.connect() as conn:
            print("MySQL连接成功！")
    except Exception as e:
        print(f"MySQL连接失败: {e}")
    finally:
        await engine.dispose()

async def test_redis_connection():
    """
    测试Redis连接
    """
    try:
        redis = await get_redis()
        await redis.ping()
        print("Redis连接成功！")
    except Exception as e:
        print(f"Redis连接失败: {e}")
    finally:
        await close_redis()

async def test_connections():
    """
    测试所有数据库连接
    """
    await test_db_connection()
    await test_redis_connection()

if __name__ == "__main__":
    asyncio.run(test_connections())
