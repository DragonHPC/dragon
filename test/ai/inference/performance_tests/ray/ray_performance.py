import ray
from ray.util.queue import Queue, Empty
from argparse import ArgumentParser
import random

random.seed(42)
import __init__
import time
import os
import socket
from dragon.ai.inference.dataset_utils import SonnetDataset
import pandas as pd


@ray.remote
class RayMetricsConsolidator:
    """
    Handles reading from the response queue and returning the response to the front end.
    Also terminates reading as soon as the expected number of inputs have been received.
    """

    def __init__(self, q, descriptor, base_start_time, excel_workbook, sheet_name):
        self.q = q
        self.descriptor = descriptor
        self.base_start_time = base_start_time
        self.excel_workbook = excel_workbook
        self.sheet_name = sheet_name

    def read_and_compute(self, num_prompts):
        """The main function for returning responses to the prompter.
        Worker idx sits inside the while loop and continuously reads from a response queue. The worker
        exits when the queue is empty and the end_ev is set.

        Args:
            num_prompts (int): The number of expected input prompts to read by the read-worker.
        """
        counter = 1

        hostname = []
        inf_worker_id = []
        devices = []
        batch_size = []

        cpu_head_network_latency = []
        guardrails_inference_latency = []
        guardrails_network_latency = []
        model_inference_latency = []
        model_network_latency = []
        end_to_end_latency = []

        requests_per_second = []
        total_tokens_per_second = []
        total_output_tokens_per_second = []

        # Read metrics.
        while True:
            try:
                response_info = self.q.get(timeout=0.1)
                response_info["counter"] = str(counter)

                # Identifiers
                hostname.append(response_info["hostname"])
                inf_worker_id.append(response_info["inf_worker_id"])
                devices.append(response_info["devices"])
                batch_size.append(response_info["batch_size"])

                # Latency
                cpu_head_network_latency.append(
                    response_info["cpu_head_network_latency"]
                )
                guardrails_inference_latency.append(
                    response_info["guardrails_inference_latency"]
                )
                guardrails_network_latency.append(
                    response_info["guardrails_network_latency"]
                )
                model_inference_latency.append(response_info["model_inference_latency"])
                model_network_latency.append(response_info["model_network_latency"])
                end_to_end_latency.append(response_info["end_to_end_latency"])

                # Throughput
                requests_per_second.append(response_info["requests_per_second"])
                total_tokens_per_second.append(response_info["total_tokens_per_second"])
                total_output_tokens_per_second.append(
                    response_info["total_output_tokens_per_second"]
                )

                if counter % 50 == 0:
                    print(
                        f"\n{response_info}\n",
                        flush=True,
                    )
                counter += 1
            except Empty:
                if (
                    counter == num_prompts + 1
                ):  # if the queue is empty and the end event is set then we shut down
                    self.total_proc_time = round(time.time() - self.base_start_time)
                    break
                else:
                    pass
            except Exception as e:
                print(f"Exception caught: {e}", flush=True)

        # Create pandas dataframe
        data = {
            # Identifiers
            "hostname": hostname,
            "inf_worker_id": inf_worker_id,
            "devices": devices,
            "dynamic_batch_size": batch_size,
            # Latency
            "cpu_head_network_latency": cpu_head_network_latency,
            "guardrails_inference_latency": guardrails_inference_latency,
            "guardrails_network_latency": guardrails_network_latency,
            "model_inference_latency": model_inference_latency,
            "model_network_latency": model_network_latency,
            "end_to_end_latency": end_to_end_latency,
            # Throughput
            "requests_per_second": requests_per_second,
            "total_tokens_per_second": total_tokens_per_second,
            "total_output_tokens_per_second": total_output_tokens_per_second,
        }
        df = pd.DataFrame(data)

        # Create new latency column that sums up everything, other than the model_inference_latency
        df["non_model_inference_latency"] = (
            df["cpu_head_network_latency"]
            + df["guardrails_inference_latency"]
            + df["guardrails_network_latency"]
            + df["model_network_latency"]
        )

        # Create column for capturing total processing time for all inputs
        df["all_inputs_combined_processing_time"] = self.total_proc_time

        # Output metrics to excel workbook
        if os.path.exists(
            self.excel_workbook
        ):  # Workbook already exists. Create sheet only
            with pd.ExcelWriter(
                self.excel_workbook,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace",
            ) as writer:
                df.to_excel(writer, sheet_name=self.sheet_name, index=False)
        else:  # Workbook does not exist.
            df.to_excel(self.excel_workbook, sheet_name=self.sheet_name, index=False)


@ray.remote
class RayInferenceWorker:
    def __init__(self, model_name, tp_size, max_batch_len, num_prompts):
        self.model_name = model_name
        self.tp_size = tp_size
        self.max_batch_len = max_batch_len
        self.hostname = socket.gethostname()
        self.node_ip = ray.util.get_node_ip_address()
        self.num_prompts = num_prompts
        self.assigned_gpus = os.environ["CUDA_VISIBLE_DEVICES"]
        self.system_prompt = ("You are a helpful chatbot", "Respond politely")
        self.sampling_params = None
        self.llm = None

    def load_model(self):
        # Lazy import vllm inside gpu proc
        from vllm import LLM, SamplingParams
        from vllm.engine.arg_utils import EngineArgs
        import dataclasses

        # Create vllm sampling args.
        self.sampling_params = SamplingParams(
            temperature=0.4,
            top_p=0.95,
            top_k=50,
            max_tokens=100,
            ignore_eos=True,
            skip_special_tokens=True,
        )

        # Load model
        engine_args = EngineArgs(
            model=self.model_name,
            tensor_parallel_size=self.tp_size,
            enforce_eager=True,
            disable_custom_all_reduce=True,
            trust_remote_code=True,
            dtype="bfloat16",
            gpu_memory_utilization=0.95,
            max_num_seqs=self.max_batch_len,
            max_model_len=1024,
        )
        self.llm = LLM(**dataclasses.asdict(engine_args))

    def generate_response(
        self,
        user_inputs,
        formatted_prompts,
        entry_timestamp,
        model_network_latency,
        response_q,
    ):
        import torch

        # Get LLM response for user prompts.
        inference_start_time = time.time()
        torch.cuda.synchronize()
        outputs = self.llm.generate(
            formatted_prompts, self.sampling_params
        )  # , use_tqdm=False)
        torch.cuda.synchronize()

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
        end_to_end_latency = round(time.time() - entry_timestamp, 2)

        response_q.put(
            {
                # Identifiers
                "hostname": self.hostname,
                "inf_worker_id": "inf_wrkr_id",
                "devices": self.assigned_gpus,
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

    def do_inference(self, input_queue, response_q):
        counter = 1

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

        while True:
            try:
                q_item = input_queue.get(block=True, timeout=0.1)
                user_inputs = q_item[0]
                formatted_prompts = [
                    chat_template_formatter(
                        self.system_prompt, prompt, [], self.model_name
                    )
                    for prompt in user_inputs
                ]
                entry_timestamp = q_item[1]
                model_network_latency = round(time.time() - entry_timestamp, 2)

                self.generate_response(
                    user_inputs,
                    formatted_prompts,
                    entry_timestamp,
                    model_network_latency,
                    response_q,
                )
                counter += 1

            except Empty:
                if counter == self.num_prompts + 1:
                    break
                else:
                    pass
            except Exception as e:
                print(f"Exception caught e: {e}", flush=True)


def run(
    head_ip,
    num_prompts,
    output_dir,
    model_name,
    tp_size,
    num_nodes,
    num_gpus_per_node,
    max_batch,
    run_type,
    model_size,
    base_descriptor,
):

    for input_batch_toggle in [True]:

        # Initialize Ray
        ray.init(address="auto", _node_ip_address=head_ip)

        # Define args
        descriptor = (
            base_descriptor + "\nSingle Input Inferece"
            if input_batch_toggle == False
            else base_descriptor + "\nBatch Input Inferece"
        )
        max_batch_len = 1 if input_batch_toggle == False else max_batch
        input_queue = Queue(maxsize=100)
        output_queue = Queue(maxsize=100)
        num_procs = (num_nodes * num_gpus_per_node) // tp_size
        print(f"\n\n{descriptor=} {max_batch_len=} {num_procs=}\n\n")

        # Load Model across all workers.
        infr_args = {
            "model_name": model_name,
            "tp_size": tp_size,
            "max_batch_len": max_batch_len,
            "num_prompts": num_prompts,
        }
        ray_infr_objs = [
            RayInferenceWorker.options(num_gpus=tp_size).remote(**infr_args)
            for _ in range(0, num_procs)
        ]
        load_model_procs = []
        for ray_infr_obj in ray_infr_objs:
            load_model_procs.append(ray_infr_obj.load_model.remote())
        print(ray.get(load_model_procs), flush=True)
        print(f"\n\nPipeline initialized!!\n\n")
        do_infr_procs = []
        for ray_infr_obj in ray_infr_objs:
            do_infr_procs.append(
                ray_infr_obj.do_inference.remote(input_queue, output_queue)
            )

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

        input_batches = 0
        start_time = time.time()
        # Insert inputs into q
        if input_batch_toggle == False:
            for prompt in samples:
                input_queue.put(([prompt], time.time()))
                input_batches += 1
        else:
            for i in range(0, len(samples), max_batch_len):
                prompt_batch = samples[i : i + max_batch_len]
                input_queue.put((prompt_batch, time.time()))
                input_batches += 1

        # Initialize Ray Read Worker
        excel_workbook = os.path.join(output_dir, f"{model_size}.xlsx")
        sheet_name = f"ray_type={run_type}_batch={input_batch_toggle}"
        reader_args = {
            "q": output_queue,
            "descriptor": descriptor,
            "base_start_time": time.time(),
            "excel_workbook": excel_workbook,
            "sheet_name": sheet_name,
        }
        reader = RayMetricsConsolidator.options(num_cpus=1).remote(**reader_args)
        consumer = reader.read_and_compute.remote(input_batches)

        # Compute and join all ray processes
        print(ray.get(consumer), flush=True)
        print(f"\n\nObtained all responses\n\n", flush=True)
        end_time = time.time()
        print(f"Total time taken: {end_time - start_time} seconds")
        # Shutdown ray
        ray.shutdown()
        time.sleep(5)
        print(f"Process completed successfully!\n\n\n")


def main(
    head_ip, model_size, num_nodes, num_gpus_per_node, run_type, num_prompts, output_dir
):

    if model_size == "small":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/hub/models--meta-llama--Llama-3.1-8B/snapshots/d04e592bb4f6aa9cfee91e2e20afa771667e1d4b"
        if run_type == "a":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=1,
                num_nodes=1,
                num_gpus_per_node=1,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Single GPU with tp_size=1.",
            )

        elif run_type == "b":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=1,
                num_nodes=1,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Multi GPU with tp_size=1.",
            )

        elif run_type == "c":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=1,
                num_nodes=2,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="2 Nodes & Multi GPU with tp_size=1.",
            )

        elif run_type == "d":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=1,
                num_nodes=num_nodes,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Multi Node & Multi GPU with tp_size=1.",
            )

    if model_size == "medium":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/llama31_70b/transformers/models--meta-llama--Llama-3.1-70B/snapshots/349b2ddb53ce8f2849a6c168a81980ab25258dac"
        if run_type == "a":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=2,
                num_nodes=1,
                num_gpus_per_node=2,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & 2 GPUs with tp_size=2.",
            )

        elif run_type == "b":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=2,
                num_nodes=1,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Multi GPU with tp_size=2.",
            )

        elif run_type == "c":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=2,
                num_nodes=2,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="2 Nodes & Multi GPU with tp_size=2.",
            )

        elif run_type == "d":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=2,
                num_nodes=num_nodes,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Multi Node & Multi GPU with tp_size=2.",
            )

    if model_size == "large":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/llama31_405b/transformers/models--hugging-quants--Meta-Llama-3.1-405B-Instruct-GPTQ-INT4/snapshots/ba9e738385d74a19443cc38235a8f647b208d938"
        if run_type == "b":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=4,
                num_nodes=1,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Single Node & Multi GPU with tp_size=4.",
            )

        elif run_type == "c":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=4,
                num_nodes=2,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="2 Nodes & Multi GPU with tp_size=4.",
            )

        elif run_type == "d":
            run(
                head_ip=head_ip,
                num_prompts=num_prompts,
                output_dir=output_dir,
                model_name=custom_model_name,
                tp_size=4,
                num_nodes=num_nodes,
                num_gpus_per_node=num_gpus_per_node,
                max_batch=60,
                run_type=run_type,
                model_size=model_size,
                base_descriptor="Multi Node & Multi GPU with tp_size=4.",
            )


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--head_ip", type=str, help="The head node's ip-addr", required=True
    )
    parser.add_argument(
        "--model_size",
        required=True,
        type=str,
        choices=["small", "medium", "large"],
        help="Determines model size you want to capture performance metrics for. Small=Llama3.1 8B; Medium=Llama3.1 70B; Large=Llama3.1 405B",
    )
    parser.add_argument(
        "--num_nodes",
        required=True,
        type=int,
        help="Number of nodes in the current allocation.",
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
        choices=["a", "b", "c", "d"],
        help="Determines run type. 'a'=Single Node & Single/Smallest GPU instance; 'b'=Single Node & Multi GPU; 'c'=2 Nodes & Multi GPU; 'd'=Multi Node & Multi GPU;",
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
        args.head_ip,
        args.model_size,
        args.num_nodes,
        args.num_gpus_per_node,
        args.run_type,
        args.num_prompts,
        args.output_dir,
    )
