import time,logging,redis
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.experiment import Experiment
from src.models.cross_trial import CrossTrial
from src.services.index_algorithm import run
from src.services import cross_trials
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
def single_trial(run_id):
 with Session() as s:
  row=s.get(Experiment,run_id)
  if not row or row.status!='queued':return
  row.status='running';snapshot=row.snapshot;s.commit()
 try:
  result=run(snapshot)
  with Session() as s:r=s.get(Experiment,run_id);r.result=result;r.status='completed';s.commit()
  log('worker','experiment.completed',str(run_id))
 except Exception as exc:
  logging.exception('trial failed')
  with Session() as s:r=s.get(Experiment,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
def cross_trial(run_id):
 with Session() as s:
  row=s.get(CrossTrial,run_id)
  if not row or row.status!='queued':return
 cross_trials.execute(run_id)
 with Session() as s:r=s.get(CrossTrial,run_id);status=r.status if r else 'missing'
 log('worker','cross_trial.'+status,str(run_id))
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:q.set('index-trial:worker-heartbeat','1',ex=30)
 except Exception:logging.exception('heartbeat')
 try:
  item=q.blpop(['cross-trials','index-trials'],timeout=2)
  if not item:continue
  queue_name,raw=item
  if queue_name==b'cross-trials':cross_trial(int(raw))
  else:single_trial(int(raw))
 except Exception:logging.exception('worker loop');time.sleep(2)
