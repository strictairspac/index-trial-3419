/** @typedef {{id:number,name:string,sensor_id:number,start_at:string,end_at:string,device_type:string|null}} Workload */
/** @typedef {{id:number,name:string,workload_id:number,columns:string[]}} Candidate */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null}} Experiment */
/** @typedef {{status:'queued'|'running'|'completed'|'failed',result:object|null,error:string|null,reused:boolean,reason:string|null,version:string|null,attempts:number}} CrossSide */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'partial'|'failed'|'mismatch',snapshot:object,sample:object,sides:{source:CrossSide,target:CrossSide},comparison:object|null,mismatch:object|null,error:string|null}} CrossTrial */
export {};
