import json,os,tempfile,time
from datetime import datetime,timedelta
from pathlib import Path
from uuid import uuid4
from sqlalchemy import select,func,update,or_,and_
from fastapi import HTTPException
from redis import Redis
from src.config.settings import REDIS_URL,ARTIFACT_DIR,RUN_LEASE_SECONDS
from src.models import RankTestRun
from src.services.study_service import get_study
from src.services.treatment_service import list_treatments
from src.services.reading_service import list_readings
from src.services.rank_inference import analyze_study
from src.constants.enums import RunStatus
redis=Redis.from_url(REDIS_URL)
def list_runs(db):return db.scalars(select(RankTestRun).order_by(RankTestRun.id.desc())).all()
def get_run(db,id):
    r=db.get(RankTestRun,id)
    if not r:raise HTTPException(404,'秩检验任务不存在')
    return r

def create_run(db,data):
    s=get_study(db,data.study_id)
    if db.scalar(select(RankTestRun).where(RankTestRun.name==data.name)):raise HTTPException(409,'任务名称重复')
    if db.scalar(select(func.count()).select_from(RankTestRun).where(RankTestRun.status.in_(['queued','running'])))>=3:raise HTTPException(429,'已有3项活动任务，请等待')
    snapshot={'study':{k:getattr(s,k) for k in ('id','code','name','protocol','source_file')},'treatments':[{k:getattr(t,k) for k in ('id','code','name','process_note')} for t in list_treatments(db,s.id)],'readings':[{k:getattr(r,k) for k in ('id','specimen_code','treatment_id','grade','source_note')} for r in list_readings(db,s.id)]}
    r=RankTestRun(**data.model_dump(),status=RunStatus.QUEUED,snapshot=snapshot);db.add(r);db.commit()
    try:redis.lpush('ordinal:runs',str(r.id))
    except Exception:pass
    return r

def _claimable(stale_before):
    return or_(RankTestRun.status==RunStatus.QUEUED,and_(RankTestRun.status==RunStatus.RUNNING,RankTestRun.claimed_at<stale_before))

def _owns_claim(db,run_id,token):
    return db.scalar(select(func.count()).select_from(RankTestRun).where(and_(RankTestRun.id==run_id,RankTestRun.status==RunStatus.RUNNING,RankTestRun.claim_token==token)))==1

def claim_next_run(db,worker_id):
    """条件更新原子认领queued或租约过期的running任务；并发下恰好一路成功。"""
    stale_before=datetime.utcnow()-timedelta(seconds=RUN_LEASE_SECONDS)
    cond=_claimable(stale_before)
    cand=db.scalar(select(RankTestRun.id).where(cond).order_by(RankTestRun.id).limit(1))
    db.rollback()
    if cand is None:return None,''
    token=f'{worker_id}:{uuid4().hex}'
    res=db.execute(update(RankTestRun).where(RankTestRun.id==cand,cond).values(status=RunStatus.RUNNING,claim_token=token,claimed_at=datetime.utcnow(),worker_id=worker_id))
    db.commit()
    if res.rowcount!=1:return None,''
    db.expire_all()
    return get_run(db,cand),token

def renew_claim(db,run_id,token):
    """续约租约；返回False说明任务已被接手，本路须放弃。"""
    res=db.execute(update(RankTestRun).where(and_(RankTestRun.id==run_id,RankTestRun.status==RunStatus.RUNNING,RankTestRun.claim_token==token)).values(claimed_at=datetime.utcnow()))
    db.commit()
    if res.rowcount:return True
    # 秒级DATETIME下同值写入可能0行，显式确认归属避免误判。
    return _owns_claim(db,run_id,token)

def write_artifact(run_id,result,artifact_dir=None):
    """临时文件+fsync+原子rename：读者不会看到写了一半的结果文件。"""
    d=Path(artifact_dir or ARTIFACT_DIR);d.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=d,prefix=f'.run-{run_id}-',suffix='.tmp')
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as fh:json.dump(result,fh,ensure_ascii=False,allow_nan=False);fh.flush();os.fsync(fh.fileno())
        os.replace(tmp,d/f'run-{run_id}.json')
    finally:Path(tmp).unlink(missing_ok=True)

def cleanup_stale_artifacts(max_age_seconds=None,artifact_dir=None):
    """清理崩溃写者留下的临时半成品；只动远超一个租约周期的文件。"""
    d=Path(artifact_dir or ARTIFACT_DIR);cutoff=time.time()-(2*RUN_LEASE_SECONDS if max_age_seconds is None else max_age_seconds)
    for p in d.glob('.run-*.tmp'):
        try:
            if p.stat().st_mtime<cutoff:p.unlink()
        except OSError:pass

def finalize_success(db,run_id,token,result):
    """先验归属再写留档，最后围栏提交；被接手的迟到计算不改留档也不改状态。"""
    owned=_owns_claim(db,run_id,token)
    db.rollback()
    if not owned:return False
    write_artifact(run_id,result)
    res=db.execute(update(RankTestRun).where(and_(RankTestRun.id==run_id,RankTestRun.status==RunStatus.RUNNING,RankTestRun.claim_token==token)).values(status=RunStatus.COMPLETED,result=result,error=''))
    db.commit()
    return res.rowcount==1

def finalize_failure(db,run_id,token,error):
    """围栏失败提交；被接手的迟到计算不能覆盖正式结果或失败状态。"""
    res=db.execute(update(RankTestRun).where(and_(RankTestRun.id==run_id,RankTestRun.status==RunStatus.RUNNING,RankTestRun.claim_token==token)).values(status=RunStatus.FAILED,error=str(error)[:600]))
    db.commit()
    return res.rowcount==1

def execute_claimed_run(db,run_id,token):
    """按冻结快照重算；只有当前认领者能提交，返回值即是否成为正式结果。"""
    r=get_run(db,run_id);snapshot,alpha=r.snapshot,r.alpha
    db.rollback()
    try:result=analyze_study(snapshot,alpha)
    except Exception as exc:return finalize_failure(db,run_id,token,exc)
    try:return finalize_success(db,run_id,token,result)
    except Exception:return False
