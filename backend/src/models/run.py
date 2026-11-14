from datetime import datetime
from typing import Optional
from sqlalchemy import String,ForeignKey,UniqueConstraint,DateTime,JSON
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.orm import Mapped,mapped_column
from src.config.database import Base
class RankTestRun(Base):
    __tablename__='rank_test_runs'
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]=mapped_column(String(140),unique=True)
    study_id:Mapped[int]=mapped_column(ForeignKey('grade_studies.id'))
    alpha:Mapped[float]=mapped_column(DOUBLE(asdecimal=False),default=.05)
    status:Mapped[str]=mapped_column(String(20),default='queued')
    snapshot:Mapped[dict]=mapped_column(JSON,default=dict)
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(String(600),default='')
    claim_token:Mapped[str]=mapped_column(String(64),default='')
    claimed_at:Mapped[Optional[datetime]]=mapped_column(DateTime,nullable=True,default=None)
    worker_id:Mapped[str]=mapped_column(String(80),default='')
    created_at:Mapped[datetime]=mapped_column(DateTime,default=datetime.utcnow)
