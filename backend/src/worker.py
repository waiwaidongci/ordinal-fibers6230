"""Multi-lane worker: atomic claim, lease takeover after interruption, fenced finalize."""
import logging,os,socket,threading,time
from pathlib import Path
from uuid import uuid4
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL,RUN_LEASE_SECONDS,WORKER_CONCURRENCY
from src.services import run_service
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
log=logging.getLogger('ordinal-worker')
WORKER_ID=f'{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}'
HEARTBEAT=Path('/tmp/heartbeat')

def _renew_loop(run_id,token,stop):
    while not stop.wait(max(RUN_LEASE_SECONDS/3,.2)):
        try:
            with Session() as db:
                if not run_service.renew_claim(db,run_id,token):log.warning('run %s: claim superseded, stop renewing',run_id);return
        except Exception:log.exception('run %s: renew failed',run_id)

def _work_once(tag):
    with Session() as db:run,token=run_service.claim_next_run(db,tag)
    if run is None:return False
    stop=threading.Event();hb=threading.Thread(target=_renew_loop,args=(run.id,token,stop),daemon=True);hb.start()
    try:
        with Session() as db:ok=run_service.execute_claimed_run(db,run.id,token)
        log.info('run %s %s by %s',run.id,'committed' if ok else 'superseded; official result kept',tag)
    except Exception:log.exception('run %s: execution error; left for takeover',run.id)
    finally:stop.set();hb.join()
    return True

def _lane_loop(tag,stop):
    redis=Redis.from_url(REDIS_URL)
    while not stop.is_set():
        try:
            if not _work_once(tag):
                try:redis.brpop('ordinal:runs',timeout=2)
                except Exception:time.sleep(2)
        except Exception:log.exception('lane %s iteration failed',tag);time.sleep(1)

def main(stop=None,lanes=None):
    stop=stop or threading.Event()
    tags=[f'{WORKER_ID}-lane{i+1}' for i in range(lanes or WORKER_CONCURRENCY)]
    threads=[threading.Thread(target=_lane_loop,args=(t,stop),daemon=True,name=t) for t in tags]
    for t in threads:t.start()
    log.info('worker %s up with %d lanes',WORKER_ID,len(threads))
    try:
        while not stop.is_set():HEARTBEAT.touch();run_service.cleanup_stale_artifacts();time.sleep(2)
    except KeyboardInterrupt:pass
    finally:
        stop.set()
        for t in threads:t.join(timeout=5)

if __name__=='__main__':main()
