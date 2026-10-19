#!/bin/bash

NUM_NODES="$1"
NUM_GPUS_PER_NODE="$2"
ROOT_DIR="$3" # root dir
NUM_PROMPTS="$4" # Set num gpus per node
EXCEL_OUT_DIR="$5"
MODEL_SIZE="$6"
HEAD_IP="$7"

echo "NUM_NODES: $NUM_NODES"
echo "NUM_GPUS_PER_NODE: $NUM_GPUS_PER_NODE"
echo "ROOT_DIR: $ROOT_DIR"
echo "NUM_PROMPTS: $NUM_PROMPTS"
echo "EXCEL_OUT_DIR: $EXCEL_OUT_DIR"
echo "MODEL_SIZE: $MODEL_SIZE"
echo "HEAD_IP: $HEAD_IP"


ssh $HEAD_IP "cd ${ROOT_DIR} && . _env/bin/activate && \
python3 tests/performance_tests/ray/ray_performance.py \
--head_ip=$HEAD_IP \
--model_size $MODEL_SIZE \
--num_nodes $NUM_NODES \
--num_gpus_per_node $NUM_GPUS_PER_NODE \
--run_type a \
--num_prompts $NUM_PROMPTS \
--output_dir $EXCEL_OUT_DIR"

ssh $HEAD_IP "cd ${ROOT_DIR} && . _env/bin/activate && \
python3 tests/performance_tests/ray/ray_performance.py \
--head_ip=$HEAD_IP \
--model_size $MODEL_SIZE \
--num_nodes $NUM_NODES \
--num_gpus_per_node $NUM_GPUS_PER_NODE \
--run_type b \
--num_prompts $NUM_PROMPTS \
--output_dir $EXCEL_OUT_DIR"

ssh $HEAD_IP "cd ${ROOT_DIR} && . _env/bin/activate && \
python3 tests/performance_tests/ray/ray_performance.py \
--head_ip=$HEAD_IP \
--model_size $MODEL_SIZE \
--num_nodes $NUM_NODES \
--num_gpus_per_node $NUM_GPUS_PER_NODE \
--run_type c \
--num_prompts $NUM_PROMPTS \
--output_dir $EXCEL_OUT_DIR"

ssh $HEAD_IP "cd ${ROOT_DIR} && . _env/bin/activate && \
python3 tests/performance_tests/ray/ray_performance.py \
--head_ip=$HEAD_IP \
--model_size $MODEL_SIZE \
--num_nodes $NUM_NODES \
--num_gpus_per_node $NUM_GPUS_PER_NODE \
--run_type d \
--num_prompts $NUM_PROMPTS \
--output_dir $EXCEL_OUT_DIR"