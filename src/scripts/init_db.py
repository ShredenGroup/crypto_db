import asyncio
from src.database import engine, Base
from src.models.kline import Period
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

async def init_period_table():
    async with AsyncSession(engine) as session:
        async with session.begin():
            # 首先检查是否已经有数据
            result = await session.execute(select(Period))
            if result.scalars().first() is not None:
                print("Period table already initialized, skipping...")
                return

            # 创建常用的K线周期
            periods = [
                Period(name="1m", minutes=1),
                Period(name="3m", minutes=3),
                Period(name="5m", minutes=5),
                Period(name="15m", minutes=15),
                Period(name="30m", minutes=30),
                Period(name="1h", minutes=60),
                Period(name="2h", minutes=120),
                Period(name="4h", minutes=240),
                Period(name="6h", minutes=360),
                Period(name="8h", minutes=480),
                Period(name="12h", minutes=720),
                Period(name="1d", minutes=1440),
                Period(name="3d", minutes=4320),
                Period(name="1w", minutes=10080),
                Period(name="1M", minutes=43200),
            ]
            session.add_all(periods)
            print("Period table initialized successfully")

async def init_database():
    try:
        # 创建表
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            print("Tables created successfully")
        
        # 初始化Period表
        await init_period_table()
        
        print("Database initialization completed!")
    except Exception as e:
        print(f"Error during database initialization: {str(e)}")
        raise
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(init_database()) 