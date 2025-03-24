from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import dotenv
import os
import asyncio
dotenv.load_dotenv()

DATABASE_URL = f"mysql+aiomysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

engine = create_async_engine(DATABASE_URL, echo=False)

async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()

async def test_connection():
    async with engine.connect() as conn:
        print("数据库连接成功！")
    # 关闭引擎
    await engine.dispose()

if __name__ == "__main__":
    print(DATABASE_URL)
    asyncio.run(test_connection())
