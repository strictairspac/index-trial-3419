import json
from fastapi import APIRouter,Depends
from fastapi.responses import Response
from src.middlewares.auth import current,writer
from src.schemas.business import CrossTrialInput
from src.services import cross_trials
from src.controllers.business import start_cross_trial,resume_cross_trial
router=APIRouter()
@router.get('/cross-trials')
def listing(u=Depends(current)):return cross_trials.list_items()
@router.post('/cross-trials')
def create(data:CrossTrialInput,u=Depends(writer)):return start_cross_trial(data,u)
@router.get('/cross-trials/{item_id}')
def detail(item_id:int,u=Depends(current)):return cross_trials.detail(item_id)
@router.post('/cross-trials/{item_id}/resume')
def resume(item_id:int,u=Depends(writer)):return resume_cross_trial(item_id,u)
@router.get('/cross-trials/{item_id}/export')
def export(item_id:int,u=Depends(current)):
 return Response(json.dumps(cross_trials.detail(item_id),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="cross-trial-{item_id}.json"'})
