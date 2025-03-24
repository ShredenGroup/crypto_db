from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from src.database import Base


class Symbol(Base):
    __tablename__ = "symbol"
    
    id = Column(Integer, primary_key=True,index=True,autoincrement=True)
    name = Column(String(20), unique=True, nullable=False)
    base_asset = Column(String(10), nullable=False)
    quote_asset = Column(String(10), nullable=False)
    status = Column(String(20), default="active")  # active, inactive
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 关系
    klines = relationship("Kline", back_populates="symbol")

class Period(Base):
    __tablename__ = "period"
    
    id = Column(Integer, primary_key=True,index=True,autoincrement=True)
    name = Column(String(10), unique=True, nullable=False)  # 1m, 5m, 15m, 1h, 4h, 1d
    minutes = Column(Integer, nullable=False)  # 对应的分钟数
    status = Column(String(20), default="active")
    created_at = Column(DateTime, default=datetime.utcnow)

class Kline(Base):
    __tablename__ = "kline"
    
    id = Column(Integer, primary_key=True)
    symbol_id = Column(Integer, ForeignKey("symbol.id"), nullable=False)
    period_id = Column(String(10), ForeignKey("period.name"), nullable=False)
    open_time = Column(DateTime, nullable=False)
    close_time = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    quote_volume = Column(Float, nullable=False)
    trade_count = Column(Integer, nullable=False)
    taker_buy_volume = Column(Float, nullable=False)
    taker_buy_quote_volume = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # 关系
    symbol = relationship("Symbol", back_populates="klines")
    
    # 索引
    __table_args__ = (
        Index('idx_symbol_period_time', symbol_id, period_id, open_time, unique=True),
        Index('idx_symbol_time', symbol_id, open_time),
    )
 
