set -e

# load environment parameters
. ./script/env.sh

# load ip list
. ./script/load_config.sh $1

script_path=${BAZEL_WORKSPACE_PATH}/scripts

if [[ -z $server ]];
then
server=//service/kv:kv_service
fi

if [[ -z $client_num ]];
then
client_num=1
fi

# obtain the src path
server_path=`echo "$server" | sed 's/:/\//g'`
server_path=${server_path:1}
server_name=`echo "$server" | awk -F':' '{print $NF}'`
server_bin=${server_name}
#grafna_port=8090

bin_path=${BAZEL_WORKSPACE_PATH}/bazel-bin/${server_path}
output_path=${script_path}/deploy/config_out
output_key_path=${output_path}/cert
output_cert_path=${output_key_path}

admin_key_path=${script_path}/deploy/data/cert

rm -rf ${output_path}
mkdir -p ${output_path}

deploy_iplist=${iplist[@]}

user="${BASELINES_USER:-vrobotin}"



echo "server src path:"${server_path}
echo "server bazel bin path:"${bin_path}
echo "server name:"${server_bin}
echo "admin config path:"${admin_key_path}
echo "output path:"${output_path}
echo "deploy to :"${deploy_iplist[@]}
echo "client num :"${client_num}

# generate keys and certificates.

cd ${script_path}
echo "where am i:"$PWD

deploy/script/generate_key.sh ${BAZEL_WORKSPACE_PATH} ${output_key_path} ${#iplist[@]}
deploy/script/generate_config.sh ${BAZEL_WORKSPACE_PATH} ${output_key_path} ${output_cert_path} ${output_path} ${admin_key_path} ${client_num} ${deploy_iplist[@]}

# build kv server
bazel build ${server} 

if [ $? != 0 ]
then
	echo "Complile ${server} failed"
	exit 0
fi

# Silent-fault mode: starting strategy is to bring up ALL N replicas first
# (so TCP handshakes complete and consensus reaches "ready"), then kill the
# last FAULTS replicas at the bottom of this script — just before
# run_performance.sh triggers client transactions.  Don't-start was broken:
# clients spin on Connection refused to the silent peers and never reach
# `is ready:1`, so consensus never starts.

# commands functions
function run_cmd(){
  count=1
  for ip in ${deploy_iplist[@]};
  do
     ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no ${user}@${ip} "source /etc/profile; $1" &
    ((count++))
  done

  while [ $count -gt 0 ]; do
        wait $pids
        count=`expr $count - 1`
  done
}

function run_one_cmd(){
  ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no ${user}@${ip} "source /etc/profile; $1" 
}

run_cmd "killall -9 ${server_bin} >/dev/null 2>&1"
run_cmd "rm -rf ${server_bin} ${server_bin}*.log server.config cert wal_log >/dev/null 2>&1"
#run_cmd "rm -rf ${server_bin}; rm -rf server.config;" 

sleep 1

# upload config files and binary
echo "upload configs"
count=0
for ip in ${deploy_iplist[@]};
do
  scp -i ${key} -r ${bin_path} ${output_path}/server.config ${output_path}/cert ${user}@${ip}:/home/${user}/  > /dev/null 2>&1 &
  #scp -i ${key} -r ${bin_path} ${output_path}/server.config ${user}@${ip}:/home/${user}/  > null 2>&1 &
  ((count++))
done

while [ $count -gt 0 ]; do
  wait $pids
  count=`expr $count - 1`
done

echo "start to run"
# Start server
idx=1
count=0
for ip in ${deploy_iplist[@]};
do
  private_key="cert/node_"${idx}".key.pri"
  cert="cert/cert_"${idx}".cert"
  run_one_cmd "FAIRDAG_BLOCK_SIZE=${FAIRDAG_BLOCK_SIZE:-} nohup ./${server_bin} server.config ${private_key} ${cert}  > ${server_bin}_${idx}.log 2>&1 &" &
  run_one_cmd "{
    echo \"startup check $(date)\";
    ls -l ./${server_bin} server.config ${private_key} ${cert} 2>&1;
    pgrep -f ${server_bin} >/dev/null 2>&1 || echo \"${server_bin} not running\";
  } > ${server_bin}_startup_${idx}.log 2>&1" &
  ((count++))
  ((idx++))
done

while [ $count -gt 0 ]; do
  wait $pids
  count=`expr $count - 1`
done

# Fetch startup logs early (helps when server fails to start)
startup_dir=${script_path}/deploy/startup_logs
mkdir -p ${startup_dir}
idx=1
count=0
for ip in ${deploy_iplist[@]};
do
  scp -i ${key} ${user}@${ip}:/home/${user}/${server_bin}_startup_${idx}.log ${startup_dir}/startup_${idx}.log > /dev/null 2>&1 &
  ((count++))
  ((idx++))
done

while [ $count -gt 0 ]; do
  wait $pids
  count=`expr $count - 1`
done

# Check ready logs
idx=1
for ip in ${deploy_iplist[@]};
do
  resp=""
  while [ "$resp" = "" ]
  do
    resp=`ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no ${user}@${ip} "grep \"receive public size:${#iplist[@]}\" ${server_bin}_${idx}.log"`
    if [ "$resp" = "" ]; then
      sleep 1
    fi
  done
  ((idx++))
done

# Silent-fault induction (FAULTS env var): now that all N replicas have
# completed key exchange and reached "receive public size:N", kill the last
# FAULTS replicas via ssh.  Clients see established connections drop —
# matching the standard BFT silent-crash model — and run_performance.sh
# below will trigger transactions against the surviving cluster.
if [[ -n "${FAULTS}" ]] && [[ "${FAULTS}" -gt 0 ]]; then
  _orig=(${iplist[@]})
  _total=${#_orig[@]}
  _client_num=${client_num:-1}
  _num_replicas=$((_total - _client_num))
  _live_replicas=$((_num_replicas - FAULTS))
  if [[ ${_live_replicas} -le 0 ]]; then
    echo "ERROR: FAULTS=${FAULTS} leaves ${_live_replicas} live replicas (N=${_num_replicas})"
    exit 1
  fi
  echo "FAULTS=${FAULTS}: killing replicas $((_live_replicas+1))..${_num_replicas} (silent-crash)"
  for ((i=_live_replicas; i<_num_replicas; i++)); do
    silent_ip=${_orig[$i]}
    ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no \
        ${user}@${silent_ip} "killall -9 ${server_bin} 2>/dev/null || true" &
  done
  wait
  sleep 1
fi

echo "Servers are running....."
