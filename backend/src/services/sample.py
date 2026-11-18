"""Frozen telemetry sample: capture once, store outside the DB, verify and restore.

A sample is bounded by an id watermark. Rows added later get higher auto-increment
ids and can never enter the frozen sample, so online writes stay possible while the
two sides remain comparable.
"""
import gzip,hashlib,json,os,uuid
from datetime import datetime,timezone
from sqlalchemy import text
from src.config.settings import ARTIFACTS_DIR
from src.models.telemetry import Telemetry
FIELDS=('id','sensor_id','recorded_at','device_type','value')
INSERT_SQL='INSERT INTO telemetry(id,sensor_id,recorded_at,device_type,value) VALUES (:id,:sensor_id,:recorded_at,:device_type,:value)'
UPDATE_SQL='UPDATE telemetry SET sensor_id=:sensor_id,recorded_at=:recorded_at,device_type=:device_type,value=:value WHERE id=:id'
DELETE_SQL='DELETE FROM telemetry WHERE id=:id'
def _canonical(rows):return hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def result_checksum(rows):return hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()
def expected_result(rows,workload):
 start,end=workload['start_at'],workload['end_at'];sensor=workload['sensor_id'];device=workload.get('device_type')
 matched=[r for r in rows if r['sensor_id']==sensor and start<=r['recorded_at']<=end and (not device or r['device_type']==device)]
 matched.sort(key=lambda r:r['id'])
 return {'rows':len(matched),'checksum':result_checksum(matched),'ids_sha':hashlib.sha256(json.dumps([r['id'] for r in matched]).encode()).hexdigest()}
def _root():return os.path.join(ARTIFACTS_DIR,'samples')
def ensure_table(engine):Telemetry.__table__.create(bind=engine,checkfirst=True)
def _read(conn,watermark):return [dict(r) for r in conn.execute(text('SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry WHERE id<=:w ORDER BY id'),{'w':watermark}).mappings()]
def _write(rows):
 os.makedirs(_root(),exist_ok=True)
 path=os.path.join(_root(),f'sample-{uuid.uuid4().hex}.json.gz')
 payload=json.dumps({'rows':rows},separators=(',',':')).encode()
 with gzip.open(path,'wb') as f:f.write(payload)
 return path,hashlib.sha256(payload).hexdigest()
def load(sample):
 with gzip.open(sample['artifact'],'rb') as f:payload=f.read()
 if hashlib.sha256(payload).hexdigest()!=sample['artifact_sha256']:raise ValueError('冻结样本文件校验失败')
 rows=json.loads(payload)['rows']
 if _canonical(rows)!=sample['fingerprint']:raise ValueError('冻结样本内容与登记指纹不符')
 return rows
def capture(engine):
 ensure_table(engine)
 with engine.connect() as conn:
  watermark=conn.execute(text('SELECT COALESCE(MAX(id),0) FROM telemetry')).scalar() or 0
  rows=_read(conn,watermark)
 if not rows:raise ValueError('源库 telemetry 没有可冻结的行')
 path,digest=_write(rows)
 return {'fingerprint':_canonical(rows),'rows':len(rows),'max_id':watermark,'fields':list(FIELDS),'artifact':path,'artifact_sha256':digest,'captured_at':datetime.now(timezone.utc).isoformat(timespec='seconds')}
def compare(live,frozen):
 a={r['id']:r for r in live};b={r['id']:r for r in frozen}
 only_live=sorted(set(a)-set(b));only_frozen=sorted(set(b)-set(a))
 changed=sorted(i for i in set(a)&set(b) if a[i]!=b[i])
 return {'only_live_ids':only_live[:50],'only_live_count':len(only_live),'missing_ids':only_frozen[:50],'missing_count':len(only_frozen),'changed_ids':changed[:50],'changed_count':len(changed)}
def verify(engine,sample,detail=False):
 ensure_table(engine)
 with engine.connect() as conn:live=_read(conn,sample['max_id'])
 frozen=load(sample) if detail else None
 diff=compare(live,frozen) if detail else {}
 return {'ok':_canonical(live)==sample['fingerprint'],'live_fingerprint':_canonical(live),'expected_fingerprint':sample['fingerprint'],'live_rows':len(live),'expected_rows':sample['rows'],'difference':diff}
def sync(engine,sample):
 """Restore rows at or below the watermark from the frozen artifact. Rows above it
 (online writes) are never touched. Repairs are verified inside the transaction and
 rolled back if the live table still does not match the registered fingerprint."""
 ensure_table(engine)
 frozen=load(sample);watermark=sample['max_id']
 with engine.connect() as conn:
  live=_read(conn,watermark)
  a={r['id']:r for r in live};b={r['id']:r for r in frozen}
  deletes=[{'id':i} for i in sorted(set(a)-set(b))]
  updates=[r for i,r in b.items() if i in a and a[i]!=r]
  inserts=[r for i,r in b.items() if i not in a]
  if deletes:conn.execute(text(DELETE_SQL),deletes)
  if updates:conn.execute(text(UPDATE_SQL),updates)
  if inserts:conn.execute(text(INSERT_SQL),inserts)
  after=_read(conn,watermark)
  if _canonical(after)!=sample['fingerprint']:
   conn.rollback()
   raise ValueError('冻结样本无法在该侧恢复到登记指纹，已回滚本次修复')
  conn.commit()
 return {'deleted':len(deletes),'updated':len(updates),'inserted':len(inserts),'verified':True,'watermark':watermark}
