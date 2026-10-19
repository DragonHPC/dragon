#!/bin/bash

# NEED TO BE UPDATED
python_env_path=/lus/scratch/kalantzi/dragon/hpc-pe-dragon-dragon
ray_script_path=/lus/scratch/kalantzi/dragon/hpc-pe-dragon-dragon/related_work/parallel_python_ray_numerical_computation

rm slurm.json
rm output.txt

# generate the slurm.json file with the network configuration
dragon-network-config --wlm slurm --output-to-json

# get a string of ip addresses from the json file
ip_list=$(python3 get_ips_from_json.py)

# Convert the space-separated string into an array
ip_array=($ip_list)

# We use the first ip address as the head node of the Ray cluster
# and the rest as the worker nodes.
# Store the first element of the array and remove it from the array
head_ip="${ip_array[0]}"
echo "Head IP address: $head_ip"

# Set the number of workers per node
num_cpus=$1

# Set up the head node of the Ray cluster
ssh $head_ip "cd ${python_env_path} && . _env/bin/activate && ray start --head --port=6379 --num-cpus=${num_cpus} --node-ip-address=${head_ip}"
echo "Head node is up"

# Set up the worker nodes of the Ray cluster
for ((i = 1; i < ${#ip_array[@]}; i++)); do
  	ip="${ip_array[$i]}"
	echo $ip
	ssh $ip "cd ${ray_script_path} && bash exec_per_node.sh ${head_ip} ${ip} ${num_cpus} ${python_env_path}"
done
