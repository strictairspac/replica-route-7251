/** Node:{id,name,role,status,lineage,position,delay_ms}; RequestSet:{id,code,title,events,note}; Trial:{id,name,request_set_id,max_delay_ms,sticky_ms,cache_ttl_ms,input_snapshot,status,result,error} */
/** Event:{at_ms,kind,session,key?,keys?,node?,position?,delay_ms?,version?}; Result.cache:{ttl_ms,summary,messages,keys,entries,changes} */
export const entityNames=['nodes','request_sets','trials'];
