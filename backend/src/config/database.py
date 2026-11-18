from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from src.config.settings import DATABASE_URL,SOURCE_DATABASE_URL,TARGET_DATABASE_URL
engine=create_engine(DATABASE_URL,pool_pre_ping=True,pool_recycle=3600)
source_engine=create_engine(SOURCE_DATABASE_URL,pool_pre_ping=True,pool_recycle=3600)
target_engine=create_engine(TARGET_DATABASE_URL,pool_pre_ping=True,pool_recycle=3600)
ENGINES={'source':source_engine,'target':target_engine}
Session=sessionmaker(bind=engine)
Base=declarative_base()
