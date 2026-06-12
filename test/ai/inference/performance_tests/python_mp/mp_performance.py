import multiprocessing as mp
import __init__
import os
import socket
import random

random.seed(42)
import time
from dragon.ai.inference.reader_utils import MetricsConsolidator
from dragon.ai.inference.dataset_utils import SonnetDataset
from queue import Empty
from argparse import ArgumentParser

def chat_template_formatter(system_prompt, user_prompt, chat_history, model_name):
    """Format the prompt using a chat template.
    :param system_prompt: System prompt to be included in the chat.
    :type system_prompt: str
    :param user_prompt: User prompt to be included in the chat.
    :type user_prompt: str
    :param chat_history: Previous chat history to be included in the prompt.
    :type chat_history: list[dict]
    :param model_name: Name of the model to use for formatting.
    :type model_name: str
    :returns: Formatted prompt string.
    :rtype: str
    """
    if len(chat_history) == 0:
        formatted_prompt = (
            f"<|system|>\n{system_prompt}\n<|user|>\n{user_prompt}\n<|assistant|>\n"
        )
    else:
        chat_context = "You remember all relevant previous chat history below."
        chat_context = str(chat_context) + str("\n".join(chat_history))
        formatted_prompt = f"<|system|>\n{system_prompt}\n{chat_context}\n<|user|>{user_prompt}\n<|assistant|>\n"
    return formatted_prompt
    
def vllm_inference(
    input_queue,
    response_q,
    end_event,
    max_batch_len,
    inf_barrier,
    devices,
    model_name,
    tp_size,
):

    # Lazy load vllm packages to ensure dragon start method maintains.
    from vllm import LLM, SamplingParams
    from vllm.engine.arg_utils import EngineArgs
    import dataclasses
    import torch
    import gc

    hostname = socket.gethostname()
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(x) for x in devices])
    system_prompt = ("You are a helpful chatbot", "Respond politely")
    print(
        f'\n\nos.environ["CUDA_VISIBLE_DEVICES"]={os.environ["CUDA_VISIBLE_DEVICES"]}\n\n'
    )

    # Create vllm sampling args.
    sampling_params = SamplingParams(
        temperature=0.5,
        top_p=0.95,
        top_k=50,
        repetition_penalty=1.1,
        stop=["<|eot_id|>", "<END>"],
        max_tokens=100,
        ignore_eos=True,
        skip_special_tokens=True,
    )

    # Load model
    engine_args = EngineArgs(
        model=model_name,
        tensor_parallel_size=tp_size,
        enforce_eager=True,
        distributed_executor_backend="mp",
        disable_custom_all_reduce=True,
        trust_remote_code=True,
        dtype="bfloat16",
        gpu_memory_utilization=0.95,
        max_num_seqs=max_batch_len,
        max_model_len=1024,
        enable_chunked_prefill=True,
    )
    llm = LLM(**dataclasses.asdict(engine_args))
    inf_barrier.wait()

    while True:
        try:
            q_item = input_queue.get(timeout=0.1)
            print("Input queue item received!!", flush=True)
            user_inputs = q_item[0]
            formatted_prompts = [
                chat_template_formatter(system_prompt, prompt, [], model_name)
                for prompt in user_inputs
            ]
            print("Prompts formatted!!", flush=True)
            entry_timestamp = q_item[1]
            model_network_latency = round(time.time() - entry_timestamp, 2)

            do_inferencing(
                hostname,
                llm,
                sampling_params,
                user_inputs,
                formatted_prompts,
                response_q,
                entry_timestamp,
                devices,
                max_batch_len,
                model_network_latency,
            )
            print("Inference done!!", flush=True)
        except Empty:
            if end_event.is_set():
                del llm
                gc.collect()
                torch.cuda.empty_cache()
                break
        except Exception as e:
            print(f"{e=}")


def do_inferencing(
    hostname,
    llm,
    sampling_params,
    user_inputs,
    formatted_prompts,
    response_q,
    input_entry_timestamp,
    devices,
    max_batch_len,
    model_network_latency,
):
    import torch

    print("Inferencing started!!", flush=True)
    # Get LLM response for user prompts.
    inference_start_time = time.time()
    torch.cuda.synchronize()
    outputs = llm.generate(formatted_prompts, sampling_params, use_tqdm=True)
    torch.cuda.synchronize()
    print("LLM generate done!!", flush=True)

    inference_elapsed_time = round(time.time() - inference_start_time, 2)

    # Calculate average tokens per second
    total_prompt_tokens = 0
    total_output_tokens = 0
    for output in outputs:
        total_prompt_tokens += (
            len(output.prompt_token_ids) if output.prompt_token_ids else 0
        )
        total_output_tokens += sum(len(o.token_ids) for o in output.outputs if o)
    total_num_tokens = total_prompt_tokens + total_output_tokens

    # Throughput metrics
    requests_per_second = round(len(outputs) / inference_elapsed_time, 2)
    total_tokens_per_second = round(total_num_tokens / inference_elapsed_time, 2)
    total_output_tokens_per_second = round(
        total_output_tokens / inference_elapsed_time, 2
    )
    cur_batch_size = len(outputs)

    cpu_head_network_latency = 0
    guardrails_network_latency = 0
    preprocessing_time = 0
    end_to_end_latency = round(time.time() - input_entry_timestamp, 2)

    response_q.put(
        {
            # Identifiers
            "hostname": hostname,
            "inf_worker_id": "inf_wrkr_id",
            "devices": devices,
            "start_time": input_entry_timestamp,
            # Batch size
            "batch_size": cur_batch_size,
            # Latency
            "cpu_head_network_latency": cpu_head_network_latency,
            "guardrails_inference_latency": preprocessing_time,
            "guardrails_network_latency": guardrails_network_latency,
            "model_inference_latency": inference_elapsed_time,
            "model_network_latency": model_network_latency,
            "end_to_end_latency": end_to_end_latency,
            # Throughput
            "requests_per_second": requests_per_second,
            "total_tokens_per_second": total_tokens_per_second,
            "total_output_tokens_per_second": total_output_tokens_per_second,
            # Output
            "user": user_inputs,
            "assistant": outputs,
        }
    )
    print("Response pushed to response queue!!", flush=True)


def run(
    num_prompts,
    output_dir,
    model_name,
    devices,
    tp_size,
    run_type,
    model_size,
    base_descriptor,
):

    for input_batch_toggle in [True]:
        descriptor = (
            base_descriptor + "\nSingle Input Inference"
            if input_batch_toggle == False
            else base_descriptor + "\nBatch Input Inference"
        )
        max_batch_len = 1 if input_batch_toggle == False else 60

        # Create ML inference workflow
        end_event = mp.Event()
        input_queue = mp.Queue()
        response_queue = mp.Queue()
        all_procs = []

        rp_event = mp.Event()

        num_procs = len(devices) // tp_size
        inf_barrier = mp.Barrier(num_procs + 1)

        for i in range(0, len(devices), tp_size):
            my_devices = devices[i : i + tp_size]
            proc = mp.Process(
                target=vllm_inference,
                args=(
                    input_queue,
                    response_queue,
                    end_event,
                    max_batch_len,
                    inf_barrier,
                    my_devices,
                    model_name,
                    tp_size,
                ),
            )
            all_procs.append(proc)

        for proc in all_procs:
            proc.start()

        inf_barrier.wait()
        print(f"\n\nPipeline initialized!!\n\n")

        # Generate arbitrary prompts of fixed length from William Shakespeare's Sonnet dataset.
        samples = SonnetDataset(
            dataset_path="inference/sonnet.dataset",
            random_seed=42,
            model_name_or_path=model_name,
            num_requests=num_prompts,
            input_len_tokens=128,
            prefix_len_tokens=48,
            return_prompt_formatted=False,
        ).sample()

        print(f"\n\nSamples generated!!\n\n")
        input_batches = (
            len(samples) // max_batch_len + 1
            if input_batch_toggle == True
            else len(samples)
        )

        print(
            f"\n\nTotal samples={len(samples)}; Total input batches={input_batches}\n\n"
        )

        # Spin up read-worker to read responses from inf-worker.
        excel_workbook = os.path.join(output_dir, f"{model_size}.xlsx")
        sheet_name = f"pythonMP_type={run_type}_batch={input_batch_toggle}"

        print(f"\n\nExcel workbook={excel_workbook}; sheet_name={sheet_name}\n\n")

        reader_end_ev = mp.Event()
        reader = MetricsConsolidator(
            response_queue,
            reader_end_ev,
            rp_event,
            descriptor,
            time.time(),
            excel_workbook,
            sheet_name,
        )
        reader_proc = mp.Process(target=reader.read_and_compute, args=(input_batches,))
        reader_proc.start()

        print(f"\n\nReader process started!!\n\n", flush=True)

        start_time = time.time()
        if input_batch_toggle == False:
            for prompt in samples:
                input_queue.put(([prompt], time.time()))
                # input_batches += 1
            rp_event.set()
        else:
            for i in range(0, len(samples), max_batch_len):
                prompt_batch = samples[i : i + max_batch_len]
                input_queue.put((prompt_batch, time.time()))
            print(f"\n\nAll samples pushed to input queue!!\n\n", flush=True)
            # input_batches += 1

            rp_event.set()

            print(
                "Setting rp_event after pushing all samples to input queue!!",
                flush=True,
            )

        # Wait here until all responses are processed by the read-worker.
        while not reader_end_ev.is_set():
            continue

        end_time = time.time()
        # Close ML inference workflow and reader.
        end_event.set()
        print(f"\n\nEnd event set!!\n\n", flush=True)
        for proc in all_procs:
            proc.join()
        reader_proc.join()
        input_queue.close()
        response_queue.close()

        print(f"\n\nAll processes joined!!\n\n", flush=True)
        time.sleep(5)
        print(f"Process completed successfully!\n\n\n", flush=True)

        print(
            f"Total time for {num_prompts} prompts = {end_time - start_time} seconds."
        )


def main(model_size, num_gpus_per_node, run_type, num_prompts, output_dir):
    all_gpu_devices = [i for i in range(0, num_gpus_per_node)]
    if model_size == "small":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/hub/models--meta-llama--Llama-3.1-8B/snapshots/d04e592bb4f6aa9cfee91e2e20afa771667e1d4b"
        if run_type == "a":
            # Single Node & Single GPU - Smallest instance of model.
            run(
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                devices=[all_gpu_devices[0]],
                tp_size=1,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Single GPU with tp_size=1.",
            )

        elif run_type == "b":
            # Single Node & Multi GPU
            run(
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                devices=all_gpu_devices,
                tp_size=1,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Multi GPU with tp_size=1.",
            )

    elif model_size == "medium":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/llama31_70b/transformers/models--meta-llama--Llama-3.1-70B/snapshots/349b2ddb53ce8f2849a6c168a81980ab25258dac"
        if run_type == "a":
            # Single Node & 2 GPUs - Smallest instance of model.
            run(
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                devices=all_gpu_devices[0:2],
                tp_size=2,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & 2 GPUs with tp_size=2.",
            )

        elif run_type == "b":
            # Single Node & Multi GPU
            run(
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                devices=all_gpu_devices,
                tp_size=2,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Multi GPU with tp_size=2.",
            )

    elif model_size == "large":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/llama31_405b/transformers/models--hugging-quants--Meta-Llama-3.1-405B-Instruct-GPTQ-INT4/snapshots/ba9e738385d74a19443cc38235a8f647b208d938"
        if run_type == "b":
            # Single Node & Multi GPU
            run(
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                devices=all_gpu_devices,
                tp_size=4,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Multi GPU with tp_size=4.",
            )


if __name__ == "__main__":
    mp.set_start_method("spawn")
    parser = ArgumentParser()
    parser.add_argument(
        "--model_size",
        required=True,
        type=str,
        choices=["small", "medium", "large"],
        help="Determines model size you want to capture performance metrics for. Small=Llama3.1 8B; Medium=Llama3.1 70B; Large=Llama3.1 405B",
    )
    parser.add_argument(
        "--num_gpus_per_node",
        required=True,
        type=int,
        help="Number of gpus per node in the current allocation.",
    )
    parser.add_argument(
        "--run_type",
        required=True,
        type=str,
        choices=["a", "b"],
        help="Determines run type. 'a'=Single Node & Single/Smallest GPU instance; 'b'=Single Node & Multi GPU; 'c'=Multi Node & Multi GPU",
    )
    parser.add_argument(
        "--num_prompts",
        required=True,
        type=int,
        help="Determines the total number of prompts that will be sent to the inference pipeline.",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        type=str,
        help="Provice the directory path where you want to save the csv files that get generated for each run.",
    )
    args = parser.parse_args()

    main(
        args.model_size,
        args.num_gpus_per_node,
        args.run_type,
        args.num_prompts,
        args.output_dir,
    )
