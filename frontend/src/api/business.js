import {api,post} from './client.js';
export const workloads={list:()=>api('/workloads'),save:(d,id)=>post('/workloads'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const candidates={list:()=>api('/candidates'),save:(d,id)=>post('/candidates'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const experiments={list:()=>api('/experiments'),run:d=>post('/experiments',d),detail:id=>api('/experiments/'+id)};
export const crossTrials={list:()=>api('/cross-trials'),run:d=>post('/cross-trials',d),detail:id=>api('/cross-trials/'+id),resume:id=>post('/cross-trials/'+id+'/resume',{})};
export const session={get token(){return sessionStorage.getItem('token')},set token(t){t?sessionStorage.setItem('token',t):sessionStorage.removeItem('token')}};
