import {workloads,candidates,experiments,crossTrials} from '../api/business.js';
export const store={workloads:[],candidates:[],experiments:[],crossTrials:[],user:null};
export async function refresh(){[store.workloads,store.candidates,store.experiments,store.crossTrials]=await Promise.all([workloads.list(),candidates.list(),experiments.list(),crossTrials.list()]);}
