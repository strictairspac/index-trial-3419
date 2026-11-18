from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey
from src.config.database import Base
class CrossTrial(Base):
 __tablename__='cross_trials'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 workload_id=Column(Integer,ForeignKey('workloads.id'),nullable=False)
 candidate_id=Column(Integer,ForeignKey('candidates.id'),nullable=False)
 status=Column(String(20),default='queued',nullable=False)
 snapshot=Column(JSON,nullable=False)
 sample=Column(JSON,nullable=False)
 sides=Column(JSON,nullable=False)
 comparison=Column(JSON,nullable=True)
 mismatch=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 created_at=Column(DateTime,server_default=func.now())
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
