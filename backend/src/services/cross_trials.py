"""Cross-version index trial: freeze one telemetry sample, measure both databases,
reuse verified side results across interruptions, and only publish a comparison when
the two result sets reconcile.
"""
import redis
from datetime import datetime,timezone
from sqlalchemy import text
from fastapi import HTTPException
from src.config.database import Session,ENGINES,source_engine,target_engine
from src.config.settings import REDIS_URL
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.cross_trial import CrossTrial
from src.services import sample as sample_service
from src.services.cross_runner import run_side,snapshot_hash,build_query,engine_identity,MEASURE_VERSION
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
SIDE_LABELS={'source':'源库','target':'目标库'}
def _blank_side():return {'status':'queued','result':None,'error':None,'attempts':0,'reused':False,'reason':None,'version':None,'checked_at':None}
def _now():return datetime.now(timezone.utc).isoformat(timespec='seconds')
def list_items():
 with Session() as s:return [record(r) for r in s.query(CrossTrial).order_by(CrossTrial.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  row=s.get(CrossTrial,item_id)
  if not row:raise HTTPException(404,'未找到跨版试验')
  return record(row)
def create(data):
 with Session() as s:
  c=s.get(Candidate,data.candidate_id)
  if not c:raise HTTPException(404,'未找到候选')
  w=s.get(Workload,c.workload_id)
  snapshot={'workload':record(w),'candidate':record(c)}
  if s.query(CrossTrial).filter_by(name=data.name).first():raise HTTPException(409,'试验编号已存在，请换一个编号')
 try:sample=sample_service.capture(source_engine)
 except Exception as exc:raise HTTPException(503,f'冻结源库样本失败：{exc}')
 sides={'source':_blank_side(),'target':_blank_side()}
 with Session() as s:
  row=CrossTrial(name=data.name,workload_id=w.id,candidate_id=c.id,status='queued',snapshot=snapshot,sample=sample,sides=sides)
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:queue.rpush('cross-trials',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(CrossTrial,result['id']);r.status='failed';r.error='队列不可用，请创建新试验';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重新执行')
 return result
def worker_alive():
 try:return bool(queue.exists('index-trial:worker-heartbeat'))
 except redis.RedisError:return False
def resume(item_id):
 with Session() as s:
  row=s.get(CrossTrial,item_id)
  if not row:raise HTTPException(404,'未找到跨版试验')
  if row.status=='running' and worker_alive():raise HTTPException(409,'试验正在执行中，请等待当前任务结束；若确认 worker 已中断请稍后重试')
  sides={k:dict(v) for k,v in row.sides.items()}
  for side in ('source','target'):
   if sides[side]['status']=='running':
    sides[side]['status']='queued';sides[side]['error']='上次执行中断，已重新排队'
  row.sides=sides
  row.status='queued';row.error=None
  s.commit();result=record(row)
 try:queue.rpush('cross-trials',item_id)
 except redis.RedisError:raise HTTPException(503,'队列不可用，请稍后重试')
 return result
def _fresh_version(engine):
 try:
  with engine.connect() as conn:return engine_identity(conn)
 except Exception:return None
def _live_sample_ok(engine,sample):
 try:
  with engine.connect() as conn:
   rows=sample_service._read(conn,sample['max_id'])
  return sample_service._canonical(rows)==sample['fingerprint']
 except Exception:return False
def _side_decision(side,state,snapshot,sample,current,live_sample_ok):
 if state['status']!='completed' or not state.get('result'):
  error=state.get('error')
  return True,(f'上次失败：{error}' if state['status']=='failed' and error else '该侧尚未完成')
 result=state['result']
 if result.get('measure_version')!=MEASURE_VERSION:return True,f"测量程序版本已更新（现为 {MEASURE_VERSION}），旧成绩需重测"
 if result.get('snapshot_hash')!=snapshot_hash(snapshot):return True,'输入快照已变化'
 if result.get('sample',{}).get('fingerprint')!=sample['fingerprint']:return True,'冻结样本指纹不一致'
 if result.get('sample',{}).get('artifact_sha256')!=sample['artifact_sha256']:return True,'冻结样本文件已变化'
 if not live_sample_ok:return True,'该侧现有样本行与冻结样本不一致，需要恢复后重测'
 if not current:return True,'无法确认该侧数据库版本'
 if result.get('engine',{}).get('version')!=current.get('version'):return True,f"实测版本 {result.get('engine',{}).get('version')} 与当前版本 {current.get('version')} 不一致"
 return False,'已验成绩可直接复用'
def _measure(engine,side,snapshot,sample):
 """One measurement, retried once if the server version changes mid-run."""
 for attempt in range(2):
  before=_fresh_version(engine)
  result=run_side(engine,snapshot,sample)
  after=_fresh_version(engine)
  if before and after and before.get('version')==after.get('version')==result['engine'].get('version'):return result
  if attempt==0:continue
  raise RuntimeError('测量期间数据库版本发生变化，结果作废')
def _compare(sides,snapshot,sample):
 frozen=sample_service.load(sample)
 expected=sample_service.expected_result(frozen,snapshot['workload'])
 src=sides['source']['result'];tgt=sides['target']['result']
 checks={
  'source_internal_equal':bool(src.get('equal_results')),
  'target_internal_equal':bool(tgt.get('equal_results')),
  'row_count_match':src.get('rows')==tgt.get('rows'),
  'checksum_match':src.get('checksum')==tgt.get('checksum'),
  'source_matches_frozen':src.get('checksum')==expected['checksum'] and src.get('rows')==expected['rows'],
  'target_matches_frozen':tgt.get('checksum')==expected['checksum'] and tgt.get('rows')==expected['rows'],
 }
 consistent=all(checks.values())
 differences=[]
 if not checks['source_internal_equal']:differences.append('源库建索引前后查询结果不一致')
 if not checks['target_internal_equal']:differences.append('目标库建索引前后查询结果不一致')
 if not checks['row_count_match']:differences.append(f"两侧返回行数不同：源库 {src.get('rows')} 行，目标库 {tgt.get('rows')} 行")
 if not checks['checksum_match']:differences.append('两侧结果 SHA-256 不一致')
 if not checks['source_matches_frozen']:differences.append('源库结果与冻结样本的期望结果不符')
 if not checks['target_matches_frozen']:differences.append('目标库结果与冻结样本的期望结果不符')
 comparison=None
 if consistent:
  comparison={'consistent':True,'expected':expected,'source':{'version':src.get('engine',{}).get('version'),'before_median_ms':src['before_median_ms'],'after_median_ms':src['after_median_ms'],'delta_ms':round(src['after_median_ms']-src['before_median_ms'],4),'speedup':round(src['before_median_ms']/src['after_median_ms'],3) if src['after_median_ms'] else None,'before_plan':src['before_plan'],'after_plan':src['after_plan'],'insert20_before_ms':src['insert20_before_ms'],'insert20_after_ms':src['insert20_after_ms'],'rows':src['rows'],'checksum':src['checksum'],'repaired':bool((src.get('sample') or {}).get('repaired'))},'target':{'version':tgt.get('engine',{}).get('version'),'before_median_ms':tgt['before_median_ms'],'after_median_ms':tgt['after_median_ms'],'delta_ms':round(tgt['after_median_ms']-tgt['before_median_ms'],4),'speedup':round(tgt['before_median_ms']/tgt['after_median_ms'],3) if tgt['after_median_ms'] else None,'before_plan':tgt['before_plan'],'after_plan':tgt['after_plan'],'insert20_before_ms':tgt['insert20_before_ms'],'insert20_after_ms':tgt['insert20_after_ms'],'rows':tgt['rows'],'checksum':tgt['checksum'],'repaired':bool((tgt.get('sample') or {}).get('repaired'))},'cross_version':{'before_median_delta_ms':round(tgt['before_median_ms']-src['before_median_ms'],4),'after_median_delta_ms':round(tgt['after_median_ms']-src['after_median_ms'],4),'plan_changed':_plan_shape(src['after_plan'])!=_plan_shape(tgt['after_plan'])}}
  return comparison,None,checks
 return None,{'summary':'两侧查询结果未对账一致，因此不发布耗时与计划的跨版对照。','differences':differences,'checks':checks,'expected':expected,'source':{'version':src.get('engine',{}).get('version'),'rows':src.get('rows'),'checksum':src.get('checksum'),'before_checksum':src.get('before_checksum'),'after_checksum':src.get('after_checksum'),'parameters':src.get('parameters')},'target':{'version':tgt.get('engine',{}).get('version'),'rows':tgt.get('rows'),'checksum':tgt.get('checksum'),'before_checksum':tgt.get('before_checksum'),'after_checksum':tgt.get('after_checksum'),'parameters':tgt.get('parameters')}},checks
def _plan_shape(plan):
 return [{'type':p.get('type'),'key':p.get('key'),'rows':p.get('rows'),'filtered':p.get('filtered')} for p in (plan or [])]
def execute(item_id):
 with Session() as s:
  row=s.get(CrossTrial,item_id)
  if not row:return
  if row.status!='queued':return
  snapshot=row.snapshot;sample=row.sample;sides={k:dict(v) for k,v in row.sides.items()}
  row.status='running';s.commit()
 for side in ('source','target'):
  engine=ENGINES[side];state=sides[side]
  current=_fresh_version(engine)
  needs,reason=_side_decision(side,state,snapshot,sample,current,_live_sample_ok(engine,sample))
  if not needs:
   state['reused']=True;state['reason']=reason;state['checked_at']=_now()
   with Session() as s:r=s.get(CrossTrial,item_id);r.sides={k:dict(v) for k,v in sides.items()};s.commit()
   continue
  state.update({'status':'running','error':None,'reused':False,'reason':reason,'attempts':state.get('attempts',0)+1})
  with Session() as s:r=s.get(CrossTrial,item_id);r.sides={k:dict(v) for k,v in sides.items()};s.commit()
  try:
   result=_measure(engine,side,snapshot,sample)
   state.update({'status':'completed','result':result,'error':None,'version':result['engine'].get('version'),'checked_at':_now()})
  except Exception as exc:
   state.update({'status':'failed','result':None,'error':str(exc)[:1000],'checked_at':_now()})
  with Session() as s:r=s.get(CrossTrial,item_id);r.sides={k:dict(v) for k,v in sides.items()};s.commit()
 with Session() as s:
  row=s.get(CrossTrial,item_id)
  sides={k:dict(v) for k,v in row.sides.items()}
  statuses=[sides[x]['status'] for x in ('source','target')]
  if all(x=='completed' for x in statuses):
   try:comparison,mismatch,checks=_compare(sides,snapshot,sample)
   except ValueError as exc:
    row.status='failed';row.comparison=None;row.mismatch=None
    row.error=f'冻结样本完整性校验失败，未发布对照：{str(exc)[:400]}';row.sides=sides;s.commit();return
   except Exception as exc:
    row.status='failed';row.comparison=None;row.mismatch=None
    row.error=f'对账计算失败：{str(exc)[:500]}';row.sides=sides;s.commit();return
   row.comparison=comparison;row.mismatch=mismatch
   row.status='completed' if comparison else 'mismatch'
   row.error=None if comparison else '两侧查询结果未对账一致'
  elif any(x=='completed' for x in statuses):
   row.status='partial';row.comparison=None;row.mismatch=None
   row.error='；'.join(f"{SIDE_LABELS[x]}：{sides[x]['error']}" for x in ('source','target') if sides[x]['status']=='failed') or '一侧尚未完成，可继续执行'
  else:
   row.status='failed';row.comparison=None;row.mismatch=None
   row.error='；'.join(f"{SIDE_LABELS[x]}：{sides[x]['error']}" for x in ('source','target')) or '两侧均未完成'
  row.sides=sides;s.commit()
