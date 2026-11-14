"""并发认领、接手围栏与写文件后中断恢复的独立验证。

默认使用临时SQLite与临时留档目录，不触碰任何真实数据：
    python -m scripts.verify_run_concurrency
对真实MySQL验证（库须已存在；只操作本脚本自建数据并在结束时清理）：
    TEST_DATABASE_URL='mysql+pymysql://user:pass@host:3306/ordinal_test?charset=utf8mb4' \
        python -m scripts.verify_run_concurrency

覆盖三情形：多路抢同一任务；接手后旧计算迟到；结果文件写出后再次中断。
"""
import collections,json,os,tempfile,threading,time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

os.environ['DATABASE_URL']=os.getenv('TEST_DATABASE_URL','sqlite:///'+os.path.join(tempfile.mkdtemp(prefix='ordinal-verify-db-'),'verify.db')+'?timeout=30')
os.environ['ARTIFACT_DIR']=os.getenv('TEST_ARTIFACT_DIR',tempfile.mkdtemp(prefix='ordinal-verify-artifacts-'))
os.environ['RUN_LEASE_SECONDS']=os.getenv('TEST_RUN_LEASE_SECONDS','0.8')
os.environ['WORKER_CONCURRENCY']='4'

from sqlalchemy import select,func,delete
from sqlalchemy.engine import make_url
from src.config.database import Base,engine,Session
from src import models
from src.models import GradeStudy,Treatment,GradeReading,RankTestRun,FollowupNote
from src.services import run_service,note_service
from src.services.rank_inference import analyze_study
from src.schemas.run import RunInput
from src.schemas.note import NoteInput,ReviewInput
from src.constants.enums import NoteStatus
from src import worker

LEASE=float(os.environ['RUN_LEASE_SECONDS']);STALE_WAIT=LEASE*1.8
ART=Path(os.environ['ARTIFACT_DIR']);SFX=uuid4().hex[:8]
FAILS=[];CREATED={'studies':[],'runs':[],'notes':[]}

def check(label,ok,detail=''):
    print(('PASS ' if ok else 'FAIL ')+label+('' if ok or not detail else f'  [{detail}]'))
    if not ok:FAILS.append(label)

def make_study(code,constant=False):
    with Session() as s:
        st=GradeStudy(code=code,name=f'验证研究 {code}',protocol='验证用独立试样；形成等级1–9仅有顺序。',source_file=f'{code}_独立纸浆试样.csv')
        s.add(st);s.flush()
        for g in range(3):
            t=Treatment(study_id=st.id,code=f'T{g+1}',name=f'工艺{g+1}',process_note='验证用工艺');s.add(t);s.flush()
            for i in range(8):
                grade=5.0 if constant else float(((i+g)%9)+1)
                s.add(GradeReading(study_id=st.id,treatment_id=t.id,specimen_code=f'{code}-T{g+1}-{i+1:03}',grade=grade,source_note=st.source_file))
        s.commit();CREATED['studies'].append(st.id);return st.id

def make_run(study_id,name,alpha=.05):
    with Session() as s:
        st=s.get(GradeStudy,study_id)
        ts=s.scalars(select(Treatment).where(Treatment.study_id==study_id).order_by(Treatment.id)).all()
        rs=s.scalars(select(GradeReading).where(GradeReading.study_id==study_id).order_by(GradeReading.id)).all()
        snapshot={'study':{k:getattr(st,k) for k in ('id','code','name','protocol','source_file')},'treatments':[{k:getattr(t,k) for k in ('id','code','name','process_note')} for t in ts],'readings':[{k:getattr(r,k) for k in ('id','specimen_code','treatment_id','grade','source_note')} for r in rs]}
        r=RankTestRun(name=name,study_id=study_id,alpha=alpha,status='queued',snapshot=snapshot)
        s.add(r);s.commit();CREATED['runs'].append(r.id);return r.id

def row_of(run_id):
    with Session() as s:
        r=s.get(RankTestRun,run_id)
        return dict(id=r.id,status=r.status,result=r.result,error=r.error,claim_token=r.claim_token,worker_id=r.worker_id,snapshot=r.snapshot,alpha=r.alpha)

def artifact_of(run_id):
    p=ART/f'run-{run_id}.json'
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None

def scenario_a():
    print('\n[A] 抢同一任务：多路并发认领，同一任务只认一份正式结果')
    sid=make_study(f'VFA-{SFX}')
    with Session() as s:r1=run_service.create_run(s,RunInput(name=f'VFY-A-真实排队-{SFX}',study_id=sid,alpha=.05));r1_id=r1.id
    CREATED['runs'].append(r1_id)
    check('A0 真实排队接口创建任务',row_of(r1_id)['status']=='queued')
    barrier=threading.Barrier(8);out=[None]*8
    def racer(i):
        with Session() as s:
            barrier.wait();run,token=run_service.claim_next_run(s,f'racer-{i}');out[i]=(run.id if run else None,token)
    threads=[threading.Thread(target=racer,args=(i,)) for i in range(8)]
    for t in threads:t.start()
    for t in threads:t.join()
    winners=[x for x in out if x and x[0] is not None]
    check('A1 8路并发抢同一任务仅1路认领成功',len(winners)==1 and winners[0][0]==r1_id,f'winners={len(winners)}')
    _,token=winners[0]
    with Session() as s:ok=run_service.execute_claimed_run(s,r1_id,token)
    expected=analyze_study(row_of(r1_id)['snapshot'],.05);row=row_of(r1_id)
    check('A2 认领者提交唯一正式结果',ok is True and row['status']=='completed' and row['result']==expected)
    check('A3 留档文件与数据库结果一致',artifact_of(r1_id)==row['result'])
    ids=[make_run(sid,f'VFY-A-积压-{i}-{SFX}') for i in range(6)]
    claims=collections.Counter();lock=threading.Lock()
    def lane(tag):
        while True:
            try:
                with Session() as s:run,tk=run_service.claim_next_run(s,tag)
                if run is None:
                    with Session() as s:remaining=s.scalar(select(func.count()).select_from(RankTestRun).where(RankTestRun.status.in_(['queued','running'])))
                    if remaining==0:return
                    time.sleep(.05);continue
                with lock:claims[run.id]+=1
                with Session() as s:run_service.execute_claimed_run(s,run.id,tk)
            except Exception:time.sleep(.1)
    lanes=[threading.Thread(target=lane,args=(f'drain-{i}',),daemon=True) for i in range(4)]
    for t in lanes:t.start()
    for t in lanes:t.join(timeout=30)
    stuck=any(t.is_alive() for t in lanes)
    check('A4 6项积压由4路全部处理完成',not stuck and all(row_of(i)['status']=='completed' for i in ids))
    check('A5 每项任务恰好被认领计算一次',all(claims[i]==1 for i in ids),str(dict(claims)))
    check('A6 积压任务结果与留档全部一致',all(row_of(i)['result']==expected and artifact_of(i)==expected for i in ids))
    before=row_of(r1_id)
    with Session() as s:forged=run_service.finalize_success(s,r1_id,'racer-loser:forged',{'fingerprint':'forged'})
    check('A7 未持令牌的迟到提交被围栏拒绝且不改留档',forged is False and row_of(r1_id)==before and artifact_of(r1_id)==expected)
    return r1_id

def scenario_b():
    print('\n[B] 接手后旧计算迟到：围栏拒绝覆盖正式结果与失败状态')
    sid=make_study(f'VFB-{SFX}');rid=make_run(sid,f'VFY-B-接手-{SFX}')
    with Session() as s:run_a,token_a=run_service.claim_next_run(s,'worker-A')
    check('B0 旧计算先认领任务',run_a is not None and run_a.id==rid)
    time.sleep(STALE_WAIT)
    with Session() as s:run_b,token_b=run_service.claim_next_run(s,'worker-B')
    check('B1 租约过期后自动接手',run_b is not None and run_b.id==rid and token_b!=token_a)
    with Session() as s:ok_b=run_service.execute_claimed_run(s,rid,token_b)
    expected=analyze_study(row_of(rid)['snapshot'],.05)
    check('B2 接手者完成并提交正式结果',ok_b is True and row_of(rid)['status']=='completed' and row_of(rid)['result']==expected)
    with Session() as s:late_ok=run_service.finalize_success(s,rid,token_a,{'fingerprint':'forged-late-result'})
    row=row_of(rid)
    check('B3 迟到的旧计算不能覆盖接手后的结果',late_ok is False and row['status']=='completed' and row['result']==expected and row['claim_token']==token_b)
    check('B4 迟到的旧计算不能改写留档文件',artifact_of(rid)==expected)
    with Session() as s:late_fail=run_service.finalize_failure(s,rid,token_a,'迟到的伪造失败');renew=run_service.renew_claim(s,rid,token_a)
    row=row_of(rid)
    check('B5 迟到的旧计算不能改写状态或续约',late_fail is False and renew is False and row['status']=='completed' and row['error']=='')
    sid_f=make_study(f'VFBF-{SFX}',constant=True);rid_f=make_run(sid_f,f'VFY-B-失败接手-{SFX}')
    with Session() as s:_,token_af=run_service.claim_next_run(s,'worker-A2')
    time.sleep(STALE_WAIT)
    with Session() as s:_,token_bf=run_service.claim_next_run(s,'worker-B2')
    with Session() as s:ok_bf=run_service.execute_claimed_run(s,rid_f,token_bf)
    row_f=row_of(rid_f)
    check('B6 接手后任务按冻结快照确定性失败',ok_bf is True and row_f['status']=='failed' and 'C=0' in row_f['error'])
    err_before=row_f['error']
    with Session() as s:late_win=run_service.finalize_success(s,rid_f,token_af,{'fingerprint':'forged-success'});late_err=run_service.finalize_failure(s,rid_f,token_af,'另一种错误')
    row_f2=row_of(rid_f)
    check('B7 迟到的旧计算不能覆盖接手后的失败状态',late_win is False and late_err is False and row_f2['status']=='failed' and row_f2['error']==err_before and row_f2['result']=={})
    return rid_f

def scenario_c(r1_id):
    print('\n[C] 结果文件写出后再次中断：恢复后页面/留档/分发同源，不露出半成品')
    expected_r1=row_of(r1_id)['result']
    with Session() as s:
        old=note_service.create_note(s,NoteInput(name=f'VFY-旧批准说明-{SFX}',run_id=r1_id,rationale='旧批准复检说明，验证恢复后保持不变。'))
        CREATED['notes'].append(old.id)
        note_service.review_note(s,old.id,ReviewInput(status=NoteStatus.APPROVED,reviewer_note='同意按说明安排复检'),SimpleNamespace(email='admin@qingnie.local'))
        csv_before=note_service.export_note(s,old.id);old_note_id=old.id
    sid=make_study(f'VFC-{SFX}');rid=make_run(sid,f'VFY-C-写后中断-{SFX}')
    expected=analyze_study(row_of(rid)['snapshot'],.05)
    with Session() as s:run_w1,_=run_service.claim_next_run(s,'worker-W1')
    row=row_of(rid)
    check('C0 第一次中断后任务保持running且无结果',run_w1 is not None and row['status']=='running' and row['result']=={})
    time.sleep(STALE_WAIT)
    with Session() as s:run_w2,_=run_service.claim_next_run(s,'worker-W2')
    run_service.write_artifact(rid,expected)
    (ART/f'.run-{rid}-junk.tmp').write_text('{"partial": "一半',encoding='utf-8')
    row=row_of(rid)
    check('C1 留档已写出但任务未完成：页面仍running、结果为空',run_w2 is not None and row['status']=='running' and row['result']=={})
    check('C2 半成品临时文件不影响正式留档，留档完整且与冻结输入复算一致',artifact_of(rid)==expected)
    with Session() as s:
        try:note_service.create_note(s,NoteInput(name=f'VFY-C-半成品说明-{SFX}',run_id=rid,rationale='试图基于未完成任务分发'));blocked=False
        except Exception as exc:blocked=getattr(exc,'status_code',None)==409
    check('C3 未完成任务不能创建复检说明（不露出半成品）',blocked)
    time.sleep(STALE_WAIT)
    with Session() as s:run_w3,token_w3=run_service.claim_next_run(s,'worker-W3')
    check('C4 再次中断后自动恢复接手',run_w3 is not None and run_w3.id==rid)
    with Session() as s:ok_w3=run_service.execute_claimed_run(s,rid,token_w3)
    row=row_of(rid)
    check('C5 恢复后结果与冻结输入复算一致',ok_w3 is True and row['status']=='completed' and row['result']==expected)
    check('C6 恢复后留档与数据库结果一致',artifact_of(rid)==row['result'])
    with Session() as s:
        note=note_service.create_note(s,NoteInput(name=f'VFY-C-复检说明-{SFX}',run_id=rid,rationale='四工艺独立复检说明，验证分发与冻结输入同源。'))
        CREATED['notes'].append(note.id)
        note_service.review_note(s,note.id,ReviewInput(status=NoteStatus.APPROVED,reviewer_note='同意复检安排'),SimpleNamespace(email='admin@qingnie.local'))
        csv_after=note_service.export_note(s,note.id)
    check('C7 批准复检分发与页面/留档同一冻结输入',expected['fingerprint'] in csv_after and row['result']['fingerprint']==expected['fingerprint'])
    run_service.cleanup_stale_artifacts(max_age_seconds=0)
    check('C8 恢复后无残留半成品临时文件',list(ART.glob('.run-*.tmp'))==[])
    row1=row_of(r1_id)
    check('C9 已完成旧任务结果未被改写',row1['status']=='completed' and row1['result']==expected_r1)
    with Session() as s:csv_old=note_service.export_note(s,old_note_id)
    check('C10 旧批准说明分发内容保持不变',csv_old==csv_before)
    return old_note_id,csv_before

def scenario_e():
    print('\n[E] 真实worker多路并发：积压处理与崩溃自动接手')
    sid=make_study(f'VFE-{SFX}');dead_id=make_run(sid,f'VFY-E-崩溃接手-{SFX}')
    with Session() as s:run_dead,_=run_service.claim_next_run(s,'dead-worker')
    check('E0 模拟崩溃worker已认领任务',run_dead is not None and run_dead.id==dead_id)
    ids=[dead_id]+[make_run(sid,f'VFY-E-积压-{i}-{SFX}') for i in range(5)]
    stop=threading.Event();wt=threading.Thread(target=worker.main,args=(stop,3),daemon=True);wt.start()
    deadline=time.time()+45;done=False
    while time.time()<deadline:
        if all(row_of(i)['status']=='completed' for i in ids):done=True;break
        time.sleep(.3)
    stop.set();wt.join(timeout=10)
    check('E1 3路worker处理完全部6项任务（含1项崩溃接手）',done)
    row=row_of(dead_id)
    check('E2 崩溃worker的任务被活着的路接手完成','lane' in row['worker_id'] and row['worker_id']!='dead-worker' and row['status']=='completed')
    check('E3 全部任务结果与冻结输入复算一致',all(row_of(i)['result']==analyze_study(row_of(i)['snapshot'],.05) for i in ids))
    check('E4 全部留档文件与数据库结果一致',all(artifact_of(i)==row_of(i)['result'] for i in ids))

def final_checks(r1_id,bf_id,old_note_id,csv_before):
    print('\n[F] 收尾：旧任务与批准说明保持不变')
    check('F1 旧完成任务保持completed且结果未变',row_of(r1_id)['status']=='completed' and row_of(r1_id)['result']['fingerprint'] in csv_before)
    row_f=row_of(bf_id)
    check('F2 旧失败任务保持failed且错误未变',row_f['status']=='failed' and 'C=0' in row_f['error'] and row_f['result']=={})
    with Session() as s:csv_now=note_service.export_note(s,old_note_id)
    check('F3 旧批准说明分发内容仍保持不变',csv_now==csv_before)
    check('F4 留档目录无半成品残留',list(ART.glob('.run-*.tmp'))==[])

def cleanup():
    try:
        with Session() as s:
            for nid in CREATED['notes']:
                n=s.get(FollowupNote,nid)
                if n:s.delete(n)
            for rid in CREATED['runs']:
                r=s.get(RankTestRun,rid)
                if r:s.delete(r)
            for sid in CREATED['studies']:
                s.execute(delete(GradeReading).where(GradeReading.study_id==sid))
                s.execute(delete(Treatment).where(Treatment.study_id==sid))
                st=s.get(GradeStudy,sid)
                if st:s.delete(st)
            s.commit()
    except Exception as exc:print('cleanup warning:',exc)

def main():
    url=make_url(os.environ['DATABASE_URL'])
    print(f'验证环境: db={url.drivername}://{url.host or "local"}/{url.database} artifacts={ART} lease={LEASE}s concurrency=4')
    Base.metadata.create_all(engine)
    try:
        r1_id=scenario_a();bf_id=scenario_b();old_note_id,csv_before=scenario_c(r1_id);scenario_e();final_checks(r1_id,bf_id,old_note_id,csv_before)
    finally:cleanup()
    print(f'\n结果: {len(FAILS)} 项失败: {FAILS}' if FAILS else '\n结果: 全部通过')
    return 1 if FAILS else 0

if __name__=='__main__':
    import sys;sys.exit(main())
