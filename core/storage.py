import sqlite3
import json
import logging
import os
from typing import List, Dict, Optional, Any
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, Text, DateTime, UniqueConstraint, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import text

# Try to import config, fallback to default
try:
    import config
    DB_URL = getattr(config, 'DB_URL', None)
except ImportError:
    DB_URL = None

Base = declarative_base()

class CubeModel(Base):
    __tablename__ = 'cubes'
    
    symbol = Column(String(20), primary_key=True)
    name = Column(String(100))
    owner_id = Column(String(50))
    owner_name = Column(String(100))
    followers_count = Column(Integer)
    total_gain = Column(Float)
    monthly_gain = Column(Float)
    daily_gain = Column(Float)
    annualized_gain_rate = Column(Float)
    description = Column(Text)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    raw_json = Column(Text)
    
    __table_args__ = (
        Index('idx_cubes_total_gain', 'total_gain'),
        Index('idx_cubes_followers', 'followers_count'),
    )

class RebalancingModel(Base):
    __tablename__ = 'rebalancing_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cube_symbol = Column(String(20))
    stock_symbol = Column(String(20))
    stock_name = Column(String(50))
    prev_weight_adjusted = Column(Float)
    target_weight = Column(Float)
    price = Column(Float)
    net_value = Column(Float)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    status = Column(String(20))
    
    __table_args__ = (
        UniqueConstraint('cube_symbol', 'stock_symbol', 'created_at', name='uq_rebalancing'),
        Index('idx_rebalancing_cube_symbol', 'cube_symbol'),
        Index('idx_rebalancing_stock_symbol', 'stock_symbol'),
        Index('idx_rebalancing_created_at', 'created_at'),
    )

class CubeStorage:
    def __init__(self, db_path: str = "data/cubes.db"):
        # Prioritize Config DB_URL
        if DB_URL:
            try:
                self.engine = create_engine(DB_URL, echo=False)
                # Test connection immediately
                with self.engine.connect() as conn:
                    pass
                logging.info(f"Connected to Database: {DB_URL.split('@')[-1]}") # Hide password
            except Exception as e:
                logging.error(f"Failed to connect to DB_URL: {e}. Fallback to SQLite.")
                self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        else:
            # Fallback to SQLite
            sqlite_url = f"sqlite:///{db_path}"
            self.engine = create_engine(sqlite_url, echo=False)
            # logging.info(f"Connected to SQLite: {db_path}")

        self.Session = sessionmaker(bind=self.engine)
        self.init_db()

    def init_db(self):
        """Initialize the database schema."""
        Base.metadata.create_all(self.engine)

    def upsert_cubes(self, cubes: List[Dict[str, Any]]):
        """Insert or update cube metadata in batch."""
        session = self.Session()
        now = datetime.now()
        
        try:
            for cube in cubes:
                symbol = cube.get("symbol")
                if not symbol: continue

                # Helper
                def parse_float(val):
                    if isinstance(val, (int, float)): return float(val)
                    if isinstance(val, str): return float(val.replace('%', '')) if val and val != "null" else 0.0
                    return 0.0

                # Prepare Data
                data = {
                    "symbol": symbol,
                    "name": cube.get("name", ""),
                    "owner_id": str(cube.get("owner", {}).get("id", "")),
                    "owner_name": cube.get("owner", {}).get("screen_name", ""),
                    "followers_count": cube.get("follower_count", 0),
                    "total_gain": parse_float(cube.get("total_gain")),
                    "monthly_gain": parse_float(cube.get("monthly_gain")),
                    "daily_gain": parse_float(cube.get("daily_gain")),
                    "annualized_gain_rate": parse_float(cube.get("annualized_gain_rate")),
                    "description": cube.get("description", ""),
                    "updated_at": now,
                    "raw_json": json.dumps(cube, ensure_ascii=False)
                }
                
                created_at_ts = cube.get("created_at", 0)
                if created_at_ts:
                    data["created_at"] = datetime.fromtimestamp(created_at_ts/1000)

                # Upsert Logic
                existing = session.query(CubeModel).filter_by(symbol=symbol).first()
                if existing:
                    for k, v in data.items():
                        if k != 'symbol' and k != 'created_at': # Don't overwrite created_at
                            setattr(existing, k, v)
                else:
                    session.add(CubeModel(**data))
            
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Error upserting cubes: {e}")
        finally:
            session.close()

    def get_existing_symbols(self) -> set:
        """Get set of all existing cube symbols."""
        session = self.Session()
        try:
            symbols = session.query(CubeModel.symbol).all()
            return {row[0] for row in symbols}
        finally:
            session.close()

    def get_cube_count(self) -> int:
        session = self.Session()
        try:
            return session.query(CubeModel).count()
        finally:
            session.close()

    def save_rebalancing_history(self, records: List[Dict[str, Any]]):
        """Save rebalancing history records to DB."""
        if not records: return

        session = self.Session()
        now = datetime.now()

        try:
            for record in records:
                if not record.get("cube_symbol") or not record.get("stock_symbol"): continue

                # Timestamp
                created_at_ts = record.get("created_at", 0)
                if isinstance(created_at_ts, (int, float)):
                    if created_at_ts > 1000000000000:
                        created_at = datetime.fromtimestamp(created_at_ts / 1000)
                    else:
                        created_at = datetime.fromtimestamp(created_at_ts)
                else:
                    created_at = now

                # Prepare Model
                data = {
                    "cube_symbol": record.get("cube_symbol"),
                    "stock_symbol": record.get("stock_symbol"),
                    "stock_name": record.get("stock_name", ""),
                    "prev_weight_adjusted": record.get("prev_weight_adjusted", 0.0),
                    "target_weight": record.get("target_weight", 0.0),
                    "price": record.get("price", 0.0),
                    "net_value": record.get("net_value", 0.0),
                    "created_at": created_at,
                    "updated_at": now,
                    "status": record.get("status", "success")
                }

                # Insert Ignore (Unique Constraint will handle dupes)
                # SQLAlchemy doesn't have native INSERT IGNORE for all DBs.
                # Check existence first
                exists = session.query(RebalancingModel).filter_by(
                    cube_symbol=data['cube_symbol'],
                    stock_symbol=data['stock_symbol'],
                    created_at=data['created_at']
                ).first()
                
                if not exists:
                    session.add(RebalancingModel(**data))
            
            session.commit()
            logging.info(f"Saved {len(records)} rebalancing records to DB.")
        except Exception as e:
            session.rollback()
            logging.error(f"Error saving rebalancing history: {e}")
        finally:
            session.close()
