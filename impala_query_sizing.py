import re
import json
import requests
import math
import csv
import sys
from os import path
import base64
import traceback

from collections import defaultdict
from datetime import datetime, timedelta

tsize_dict = {
              '0_2' : 'XSMALL',
              '3_10' : 'SMALL',
              '11_20' : 'MEDIUM',
              '21_40' : 'LARGE',
              '41_999' : 'CUSTOM'
            }

tsize_dict_cache = {
                     '0_400' : 'XSMALL',
                     '401_2000' : 'SMALL',
                     '2001_4000' : 'MEDIUM',
                     '4001_8000' : 'LARGE',
                     '8001_99999' : 'CUSTOM'
                   }

resource_events = list()
qoffset = 0
skipped_queries = []
config = {}
total_queries = 0
min_executor_pod_workload = 0
size_matrix = defaultdict(lambda: defaultdict(int))
total_query_time = float(0)
util_mem = float(0)
util_cpu = float(0)
util_cache = float(0)
util_spill = float(0)
max_pods_workload_start = ''
max_pods_query_id = ''
input_file = ''
max_vcores = float(0)
max_backends = int(0)
max_mem = float(0)
max_data = float(0)
max_data_rate = float(0)
max_spill = float(0)
prune_csvwriter = None
prune_count = int(0)
pools = set()
query_id=''


#Check if config file parameter is provided to run the script
if len(sys.argv) != 2:
	print ("ERROR: Config file parameter required")
	sys.exit()
#else:
#	print ("Number of args=", len(sys.argv))

#Check if config file exists
if path.exists(sys.argv[1]) == False:
	print ("Config file doesn't exist")
	sys.exit()
else:
	print ("Config file: "+ sys.argv[1])

def get_tsize(min_executor_pod,ttype):
	min_executor_pod_rounded = int(math.ceil(min_executor_pod))
	if ttype == 'cache':
		for key in tsize_dict_cache.keys():
	  		pod_range =  key.split('_')
	  		#print("Start_range="+pod_range[0]+", End_range="+pod_range[1])
	  		if min_executor_pod_rounded >= int(pod_range[0]) and min_executor_pod_rounded <= int(pod_range[1]):
				return tsize_dict_cache[key]
 	else:
   		for key in tsize_dict.keys():
	  		pod_range =  key.split('_')
	  		#print("Start_range="+pod_range[0]+", End_range="+pod_range[1])
	  		if min_executor_pod_rounded >= int(pod_range[0]) and min_executor_pod_rounded <= int(pod_range[1]):
				return tsize_dict[key]

try:
	#Read all configuration from config file into a dictionary
	with open(sys.argv[1]) as cfile:
		for line in cfile.readlines():
			#print(line.rstrip("\n").split('='))
			key, value = line.rstrip("\n").split('=')
			config[key] = value

	#Read password from a property file
	f = open(config['pfile'], 'r')
	passwd = base64.b64decode(f.read().rstrip()).decode("utf-8")

	#query_sla_sec = int(config['query_sla_sec'])
	to_date = config['to']
	from_date = config['from']

	cache_adjustment_pct = int(config['cache_adjustment_pct'])
	mem_adjustment_pct = int(config['mem_adjustment_pct'])
	cpu_adjustment_pct = int(config['cpu_adjustment_pct']) 
	
	scratch_gb_per_node = int(config['scratch_gb_per_node'])
	cache_gb_per_node = int(config['cache_gb_per_node'])
	query_mem_per_node = int(config['query_mem_per_node'])
	vcores_per_node = int(config['vcores_per_node'])
	pod_limit = int(config['pod_limit'])
	mt_dop = int(config['mt_dop'])
	mt_scaling_factor = round(0.93**(mt_dop-1)*mt_dop,2)
	if 'input_file' in config:
		input_file = config['input_file']

	if 'pool' in config:
		conf_pool = config['pool']
	

	fields = [ 'query_id', 'pool','start_time','end_time','duration_millis','reqd_cache_gb','min_exec_pod_cache','tsize_cache','reqd_agg_mem','min_exec_pod_mem','tsize_mem','cpu_time_sec','query_sla_sec', 'reqd_parallelism_cpu','min_exec_pod_cpu','tsize_cpu', 'memory_spilled_gb', 'in_executor_pod_spill', 'tsize_spill', 'min_executor_pod', 'recommended_tsize', 'query_type', 'admission_wait', 'num_backends']

	with open( config['output_file'], 'w') as csvfile:
		csvwriter = csv.writer(csvfile,delimiter=',' , quoting=csv.QUOTE_NONNUMERIC)
		csvwriter.writerow(fields)

		while True:
			
			if input_file != '':
				print ("Input File =>"+input_file)
				doc_list = [ doc for doc in csv.DictReader(open( input_file, 'r').readlines()) ]
				query_count = 1
				warning_count = 0
			else:
				try:
					
					#API call to get Impala query stats for specific time window.
					#print (config['cm_url']+'/api/v32/clusters/'+config['cluster_name']+'/services/impala/impalaQueries?from='+from_date+'&to='+to_date+'&filter=queryType=QUERY and executing=false&limit=1000&offset='+str(qoffset))
					if conf_pool != '':		
						response = requests.get(config['cm_url']+'/api/v32/clusters/'+config['cluster_name']+'/services/impala/impalaQueries?from='+from_date+'&to='+to_date+'&filter=queryType=QUERY and executing=false and pool='+conf_pool+'&limit=1000&offset='+str(qoffset), auth=(config['user_name'],passwd))
					else:
						 response = requests.get(config['cm_url']+'/api/v32/clusters/'+config['cluster_name']+'/services/impala/impalaQueries?from='+from_date+'&to='+to_date+'&filter=queryType=QUERY and executing=false&limit=1000&offset='+str(qoffset), auth=(config['user_name'],passwd))
					response.raise_for_status()
				except requests.exceptions.RequestException as err:
					print ("ERROR: API request failed with error: ", err )
					raise SystemExit(err)
			
				query_count = len( response.json()['queries'])

				print ("QueryCount =" , query_count)
				warning_count = len( response.json()['warnings'])

				if query_count < 1000 and warning_count == 1:
					warn_string = response.json()['warnings'][0].split()
					to_date = warn_string[-1]
					print ( "New To_Date => " + to_date )
					qoffset=0
					if query_count == 0:
						continue
				else:
					qoffset += 1000

				print("Offset =", qoffset)
				
				#prepare list of all queries
				doc_list = [ doc for doc in response.json()['queries'] ]

			for doc in doc_list:
				#total_queries = total_queries + 1
				#print( "Doc => " + str(doc) )
				#query_id = doc['query_id']
				#print ("Query Id =>" + query_id )
				if input_file != '':
					#print ("Query Id =>" + query_id )
					query_id = doc['query_id']
					pool = doc['pool']
					query_start_time = doc['start_time']
					query_end_time = doc['end_time']
					query_duration = long(doc['duration_millis'])
					query_state =  'dummy_state'
					user_id = 'dummy_user'
					hdfs_bytes_read_gb = float(doc['reqd_cache_gb'])
					mem_agg_peak_gb = float(doc['reqd_agg_mem'])
					memory_spilled_gb = float(doc['memory_spilled_gb'])
					cpu_time_sec = float(doc['cpu_time_sec'])
					query_type = doc['query_type']
					admission_wait_ms = int(doc['admission_wait'])
					num_backends = int(doc['num_backends'])
				else:
					query_id = doc['queryId']
					query_start_time = doc['startTime']
					query_end_time = doc['endTime']
					query_duration = long(doc['durationMillis'])
					query_state =  doc['queryState']
					user_id = doc['user']
                                        query_type = doc['queryType']

					if 'memory_aggregate_peak' not in doc['attributes']:
						skipped_queries.append(query_id+'|'+str(query_duration)+'|'+query_start_time+'|'+query_end_time+'|'+query_state)
						#print ("Skipped Query => " + doc['query_id'])
						continue

					pool = doc['attributes']['pool']
					hdfs_bytes_read_gb = round((float(doc['attributes']['hdfs_bytes_read'])/1024/1024/1024),2)
					mem_agg_peak_gb = round(float(doc['attributes']['memory_aggregate_peak'])/1024/1024/1024,2)
					memory_spilled_gb = round(float(doc['attributes']['memory_spilled'])/1024/1024/1024,2)
					cpu_time_sec = round(float(doc['attributes']['thread_cpu_time'])/1000,2)
					admission_wait_ms = int(doc['attributes']['admission_wait'])
					num_backends = int(doc['attributes']['num_backends'])
				

				if 'pool' in config and conf_pool != pool and conf_pool !='':	
					continue
				
				else:
					total_queries = total_queries + 1
					pools.add(pool)

				#Thread cpu time in Millisecond.
				duration_sec = float(query_duration)/1000
				query_sla_sec = round(duration_sec,2)
				min_parallelism = int(math.ceil(cpu_time_sec/duration_sec))
				#print (" Min parallelism => ",  min_parallelism )
				avg_vcores = round(float(min_parallelism) / num_backends,2)
				avg_mem = round(float(mem_agg_peak_gb) / num_backends,2)
				avg_data = round(float(hdfs_bytes_read_gb) / num_backends,2)
				avg_data_rate = round(float(hdfs_bytes_read_gb) / num_backends / duration_sec,2)
				avg_spill = round(float(memory_spilled_gb) / num_backends,2)
				#vcores_limited = min(vcores_per_node,avg_vcores)
				vcores_limited = vcores_per_node
				parallel_factor = max(mt_scaling_factor, vcores_limited)


				#print("Query_Id => "+query_id+","+query_start_time+","+query_end_time+","+str(query_duration/1000)+","+str(mem_agg_peak_gb) +","+str(cpu_time_sec)+","+ str(hdfs_bytes_read_gb))

				min_executor_pod_data = hdfs_bytes_read_gb/cache_gb_per_node
				min_executor_pod_data_rounded = int(math.ceil(min_executor_pod_data))
				
				#print ("Executor Pod Cache"  ,min_executor_pod_data_rounded)

				min_executor_pod_mem = ((mem_agg_peak_gb*mem_adjustment_pct)/100)/query_mem_per_node
				min_executor_pod_mem_rounded = int(math.ceil(min_executor_pod_mem))
				#print ("Executor Pod Memory"  ,min_executor_pod_mem_rounded)

				min_executor_pod_cpu = ((cpu_adjustment_pct*min_parallelism)/100)/parallel_factor
				min_executor_pod_cpu_rounded = int(math.ceil(min_executor_pod_cpu))
				#print ("Executor Pod CPU"  ,min_executor_pod_cpu_rounded)

				min_executor_pod_spill = memory_spilled_gb/scratch_gb_per_node
				min_executor_pod_spill_rounded = int(math.ceil(min_executor_pod_spill))
				#print ("Executor Pod Spill"  ,min_executor_pod_spill_rounded)

				tsize_data = get_tsize(min_executor_pod_data_rounded,'cache_new')
				tsize_mem = get_tsize(min_executor_pod_mem_rounded,'memory')
				tsize_cpu = get_tsize(min_executor_pod_cpu_rounded,'cpu')
				tsize_spill = get_tsize(min_executor_pod_spill_rounded,'spill')

				min_executor_pod = max(min_executor_pod_data,min_executor_pod_mem,min_executor_pod_cpu,min_executor_pod_spill)
				min_executor_pod_rounded = int(math.ceil(min_executor_pod))
				recommended_tsize = get_tsize(min_executor_pod,'overall')
				row = [ query_id,pool,query_start_time,query_end_time,query_duration,hdfs_bytes_read_gb,min_executor_pod_data_rounded, tsize_data, mem_agg_peak_gb, min_executor_pod_mem_rounded, tsize_mem, cpu_time_sec, query_sla_sec, min_parallelism, min_executor_pod_cpu_rounded, tsize_cpu, memory_spilled_gb, min_executor_pod_spill_rounded, tsize_spill, min_executor_pod_rounded, recommended_tsize, query_type, admission_wait_ms, num_backends ]

				if min_executor_pod_rounded > pod_limit:
					if prune_csvwriter is None:
						prune_csvfile = open( config['prune_output_file'], 'w')
						prune_csvwriter = csv.writer(prune_csvfile,delimiter=',' , quoting=csv.QUOTE_NONNUMERIC)
						prune_csvwriter.writerow(fields)
					prune_csvwriter.writerow(row)
					prune_count += 1
					continue

				# Update any global stats after pruning
				if min_executor_pod_rounded > min_executor_pod_workload:
					min_executor_pod_workload = min_executor_pod_rounded
					max_pods_query_id = query_id

				if num_backends > max_backends:
					max_backends = num_backends

				if avg_vcores > max_vcores:
					max_vcores = avg_vcores

				if avg_mem > max_mem:
					max_mem = avg_mem

				if avg_data > max_data:
					max_data = avg_data

				if avg_data_rate > max_data_rate:
					max_data_rate = avg_data_rate

				if avg_spill > max_spill:
					max_spill = avg_spill

				size_matrix[recommended_tsize]['count'] += 1
				size_matrix[tsize_data]['cache'] += 1
				size_matrix[tsize_mem]['mem'] += 1
				size_matrix[tsize_cpu]['cpu'] += 1
				size_matrix[tsize_spill]['spill'] += 1

				# TODO: compute total_query_time in events loop below
				total_query_time += float(query_duration-admission_wait_ms)/1000
				util_mem += (mem_agg_peak_gb * duration_sec)
				util_cpu += cpu_time_sec
				util_cache += (hdfs_bytes_read_gb * duration_sec)
				util_spill += (memory_spilled_gb * duration_sec)

				adjust_msec = int(query_start_time[20:23])+admission_wait_ms
				timedelta(seconds = adjust_msec/1000)
				admitted_dt = datetime.strptime(query_start_time[:19], '%Y-%m-%dT%H:%M:%S')+timedelta(seconds = adjust_msec/1000)
				admitted_ts=admitted_dt.strftime('%Y-%m-%dT%H:%M:%SZ')+str(adjust_msec%1000)
				start_resources = {
				     'timestamp' : admitted_ts,
				     'count' : 1,
				     'overall' : min_executor_pod,
				     'cache' : hdfs_bytes_read_gb/num_backends,
				     'mem' : mem_agg_peak_gb/num_backends,
				     'cpu' : avg_vcores,
				     'data_rate' : avg_data_rate,
				     'spill' : memory_spilled_gb/num_backends
				}
				resource_events.append(start_resources)

				end_resources = {
				     'timestamp' : query_end_time,
				     'count' : -1,
				     'overall' : -min_executor_pod,
				     'cache' : -hdfs_bytes_read_gb/num_backends,
				     'mem' : -mem_agg_peak_gb/num_backends,
				     'cpu' : -avg_vcores,
				     'data_rate' : -avg_data_rate,
				     'spill' : -memory_spilled_gb/num_backends
				}
				resource_events.append(end_resources)


				csvwriter.writerow(row)
			
			if query_count < 1000 and warning_count == 0:
				break		
			
	with open( config['skip_query_file'], 'w') as qfile:
		qwriter = csv.writer(qfile,delimiter='\n' , quoting=csv.QUOTE_NONNUMERIC)
		qwriter.writerow(['query_id | query_duration'])
		qwriter.writerow(skipped_queries)
except:
	#e = sys.exc_info()[0]
	print ("ERROR: {} occured on line {} ".format( sys.exc_info()[0], sys.exc_info()[-1].tb_lineno))
	traceback.print_exc()	
	print ('Query_id => '+query_id)

def bytimestamp(resource_event):
	return resource_event['timestamp']

max_concurrent_queries = 0
max_pods_workload = float(0)
max_concurrent_memory = float(0);
max_concurrent_cores = float(0);
max_concurrent_cache = float(0);
max_concurrent_spill = float(0);
max_concurrent_data_rate = float(0);

sum_query = 0
sum_pods = float(0)
sum_mem = float(0)
sum_cpu = float(0)
sum_cache = float(0)
sum_spill = float(0)
sum_data_rate = float(0)

tsize_workload = get_tsize(min_executor_pod_workload,"")

resource_events.sort(key=bytimestamp)
for re in resource_events:
	sum_pods += re['overall']
	sum_query += re['count']
	sum_cache += re['cache']
	sum_mem += re['mem']
	sum_cpu += re['cpu']
	sum_spill += re['spill']
	sum_data_rate += re['data_rate']
	if(re['count'] > 0):
		if sum_query > max_concurrent_queries:
			max_concurrent_queries = sum_query
		if sum_pods >= max_pods_workload:
			max_pods_workload = sum_pods
			max_pods_workload_start = re['timestamp']
		if sum_cache > max_concurrent_cache:
			max_concurrent_cache = sum_cache
		if sum_mem > max_concurrent_memory:
			max_concurrent_memory = sum_mem
		if sum_cpu > max_concurrent_cores:
			max_concurrent_cores = sum_cpu
		if sum_spill >  max_concurrent_spill:
			max_concurrent_spill = sum_spill
		if sum_data_rate >  max_concurrent_data_rate:
			max_concurrent_data_rate = sum_data_rate


print ""
print "Individual Query Analysis"
print " Total Queries:",total_queries
print " Total Query Time:",round(total_query_time,2),"sec"
#print " MT Scaling factor:",mt_scaling_factor 
print " Highest Resources Query ID:",max_pods_query_id
print " Max Nodes:",max_backends
print " Max Cores Per Node:",max_vcores
print " Max Data Per Node:",max_data,"GB"
print " Max Spill Per Node:",max_spill,"GB"
print " Max Memory Per Node:",max_mem,"GB/s"
print " Max Data Rate:",max_data_rate,"GB"
print " Pools:"
for pool in pools:
	print "  ",pool
if prune_count > 0:
	print " Queries Over Pod Limit (",pod_limit,"):",prune_count

print ""
print "Concurrent Query Analysis"
print " Max Concurrent Queries:",max_concurrent_queries
print " Max Concurrent Resources Time:",max_pods_workload_start
print " Max Concurrent Cores Per Node:",max_concurrent_cores
print " Max Concurrent Data Per Node:",round(max_concurrent_cache,2),"GB"
print " Max Concurrent Spill Per Node:",round(max_concurrent_spill,2),"GB"
print " Max Concurrent Memory Per Node:",round(max_concurrent_memory,2),"GB"
print " Max Concurrent Data Rate:",max_concurrent_data_rate,"GB/s"


size_wl = size_matrix[tsize_workload]
constrained_by = ""

for dim in { 'cache', 'mem', 'cpu', 'spill' }:
	if(size_wl[dim] > 0):
		constrained_by += (dim+" ")
print ""
print "			    Cluster Sizing"
print "Size		Min Pods	Max Pods	Constrained By"
print tsize_workload,"		",min_executor_pod_workload,"		",int(math.ceil(max_pods_workload)),"		",constrained_by

print ""
print "			    Query Counts"
print "                     Cache       Mem         CPU         Spill"
print "Size     Count       Constrained Constrained Constrained Constrained"
for tsize in [ 'XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'CUSTOM' ]:
        size_row = tsize.rjust(8)
	for dim in [ 'count', 'cache', 'mem', 'cpu', 'spill' ]:
		size_row += (" "+str(size_matrix[tsize][dim]).rjust(11))
 	print size_row

mem_util_pct = round(100*util_mem / (min_executor_pod_workload * query_mem_per_node * total_query_time),2)
cpu_util_pct = round(100*util_cpu / (min_executor_pod_workload * vcores_per_node * total_query_time),2)
#cpu_util_pct = round(100*util_cpu / (min_executor_pod_workload * mt_scaling_factor * total_query_time),2)
cache_util_pct = round(100*util_cache / (min_executor_pod_workload * cache_gb_per_node * total_query_time),2)
spill_util_pct = round(100*util_spill / (min_executor_pod_workload * scratch_gb_per_node * total_query_time),2)

print ""
print "			    Average Cluster Utilization"
print "Cache    Memory    CPU       Spill"
print str(cache_util_pct).rjust(6),"% ",str(mem_util_pct).rjust(6),"% ",str(cpu_util_pct).rjust(6),"% ",str(spill_util_pct).rjust(6),"%"

