"""Measure one database side against one frozen telemetry sample.

Both sides execute the same recorded query, which is the workload predicate plus the
frozen-sample watermark (id <= max_id). The watermark is what keeps results stable
while production keeps writing rows with higher auto-increment ids.
"""
import time,statistics,json,hashlib
from datetime import datetime,timezone
from sqlalchemy import text
from src.services import sample as sample_service
INDEX_NAME='trial_candidate'
ROUNDS=5
PROBE_ROWS=20
# Bump when the measurement procedure changes so stored side results are never reused.
MEASURE_VERSION='cross-runner/3'
INSERT_PROBE="INSERT INTO telemetry(sensor_id,recorded_at,device_type,value) VALUES (999,'2026-11-17T12:00:00','pump',42.5)"
def snapshot_hash(snapshot):return hashlib.sha256(json.dumps(snapshot,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def build_query(workload,watermark=False):
 query='SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<=:end'
 if workload.get('device_type'):query+=' AND device_type=:device'
 if watermark:query+=' AND id<=:watermark'
 return query+' ORDER BY id'
def build_parameters(workload):
 params={'sensor':workload['sensor_id'],'start':workload['start_at'],'end':workload['end_at']}
 if workload.get('device_type'):params['device']=workload['device_type']
 return params
def engine_identity(conn):return {'version':conn.execute(text('SELECT VERSION()')).scalar(),'comment':conn.execute(text('SELECT @@version_comment')).scalar()}
def _timed(conn,query,params):
 samples=[];rows=[]
 for _ in range(ROUNDS):
  started=time.perf_counter();rows=[dict(r) for r in conn.execute(text(query),params).mappings()];samples.append((time.perf_counter()-started)*1000)
 return rows,round(statistics.median(samples),4),[round(x,4) for x in samples]
def _write_probe(conn):
 started=time.perf_counter()
 conn.execute(text(INSERT_PROBE),[{} for _ in range(PROBE_ROWS)])
 elapsed=(time.perf_counter()-started)*1000
 conn.rollback()
 return round(elapsed,4)
def _index_present(conn):
 return conn.execute(text("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='telemetry' AND index_name=:n"),{'n':INDEX_NAME}).scalar()
def _ddl(conn,sql):
 try:
  conn.execute(text(sql+' ALGORITHM=INPLACE LOCK=NONE'));conn.commit();return 'inplace-lock-none'
 except Exception:
  conn.rollback();conn.execute(text(sql));conn.commit();return 'blocking'
def run_side(engine,snapshot,sample):
 workload=snapshot['workload'];columns=snapshot['candidate']['columns']
 query=build_query(workload,watermark=True)
 params={**build_parameters(workload),'watermark':sample['max_id']}
 sample_service.ensure_table(engine)
 with engine.connect() as conn:
  if conn.execute(text("SELECT GET_LOCK('index_trial_serial',30)")).scalar()!=1:raise RuntimeError('另一个试验正占用该侧数据库的试验锁')
  try:
   identity=engine_identity(conn)
   verified=sample_service.verify(engine,sample)
   repairs=None
   repaired=False
   if not verified['ok']:
    repairs=sample_service.sync(engine,sample);repaired=True
    verified=sample_service.verify(engine,sample)
    if not verified['ok']:raise RuntimeError('冻结样本无法在该侧恢复')
   if _index_present(conn):
    conn.execute(text('DROP INDEX '+INDEX_NAME+' ON telemetry'));conn.commit()
   before_plan=[dict(r) for r in conn.execute(text('EXPLAIN '+query),params).mappings()]
   before_rows,before_ms,before_samples=_timed(conn,query,params)
   write_before=_write_probe(conn)
   ddl_mode=_ddl(conn,'CREATE INDEX '+INDEX_NAME+' ON telemetry ('+','.join('`'+c+'`' for c in columns)+')')
   conn.execute(text('ANALYZE TABLE telemetry')).all()
   after_plan=[dict(r) for r in conn.execute(text('EXPLAIN '+query),params).mappings()]
   after_rows,after_ms,after_samples=_timed(conn,query,params)
   write_after=_write_probe(conn)
   before_checksum=sample_service.result_checksum(before_rows)
   after_checksum=sample_service.result_checksum(after_rows)
   return {'measure_version':MEASURE_VERSION,'engine':identity,'snapshot_hash':snapshot_hash(snapshot),'sample':{'fingerprint':verified['live_fingerprint'],'rows':verified['live_rows'],'max_id':sample['max_id'],'artifact_sha256':sample['artifact_sha256'],'repairs':repairs,'repaired':repaired},'query':query,'workload_query':build_query(workload),'parameters':params,'rows':len(after_rows),'before_plan':before_plan,'after_plan':after_plan,'before_median_ms':before_ms,'after_median_ms':after_ms,'before_samples_ms':before_samples,'after_samples_ms':after_samples,'insert20_before_ms':write_before,'insert20_after_ms':write_after,'equal_results':before_checksum==after_checksum,'before_checksum':before_checksum,'after_checksum':after_checksum,'checksum':after_checksum,'preview':after_rows[:10],'ddl_mode':ddl_mode,'measured_at':datetime.now(timezone.utc).isoformat(timespec='seconds'),'limitations':['the trial query adds the frozen-sample watermark id<=max_id so concurrent production writes cannot change the compared rows','numeric timings include Python driver and cache effects; no guaranteed speedup','20-row write probe is rolled back; auto-increment gaps are normal','index is dropped after measurement']}
  finally:
   conn.rollback()
   try:
    if _index_present(conn):
     try:_ddl(conn,'DROP INDEX '+INDEX_NAME+' ON telemetry')
     except Exception:conn.rollback()
   finally:
    conn.execute(text("SELECT RELEASE_LOCK('index_trial_serial')"));conn.commit()
