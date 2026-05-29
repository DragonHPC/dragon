import os

from dragon.native.queue import Queue
from dragon.native.event import Event
from dragon.native.process import Process
import __init__
from argparse import ArgumentParser
import yaml
from dragon.ai.inference.dataset_utils import SonnetDataset
from dragon.ai.inference.inference_utils import Inference
from dragon.ai.inference.reader_utils import MetricsConsolidator
from dragon.ai.inference.config import InferenceConfig
import time
import copy
from pprint import pprint


def update_config(
    base_config,
    input_batch_toggle,
    custom_model_name,
    custom_num_nodes,
    custom_num_gpus,
    custom_tp_size,
    run_type,
):
    """Update base configuration with custom parameters for the performance tests.

    :param base_config: Base configuration dictionary.
    :type base_config: dict
    :param input_batch_toggle: Toggle for input batching.
    :type input_batch_toggle: bool
    :param custom_model_name: Custom model name or path.
    :type custom_model_name: str
    :param custom_num_nodes: Custom number of nodes.
    :type custom_num_nodes: int
    :param custom_num_gpus: Custom number of GPUs.
    :type custom_num_gpus: int
    :param custom_tp_size: Custom tensor parallel size.
    :type custom_tp_size: int
    :returns: Updated configuration dictionary.
    :rtype: dict
    """
    config = copy.deepcopy(base_config)
    config["required"]["model_name"] = custom_model_name
    config["hardware"]["num_nodes"] = custom_num_nodes
    config["hardware"]["num_gpus"] = custom_num_gpus
    # config["hardware"]["num_inf_wrkrs_per_cpu"] = 1
    config["required"]["tp_size"] = custom_tp_size
    config["input_batching"]["toggle_on"] = True
    config["input_batching"]["type"] = "pre-batch"
    config["guardrails"]["toggle_on"] = False
    config["dynamic_inf_wrkr"]["toggle_on"] = False
    config.setdefault("run_type", run_type)
    config.setdefault("token", "")
    pprint(config)
    return config


def run(
    config_file,
    num_prompts,
    output_dir,
    custom_model_name,
    custom_num_nodes,
    custom_num_gpus,
    custom_tp_size,
    run_type,
    model_size,
    custom_descriptor,
):
    """Run performance tests for the Dragon inference pipeline.

    :param config_file: Path to the configuration file.
    :type config_file: str
    :param num_prompts: Number of prompts to send to the inference pipeline.
    :type num_prompts: int
    :param output_dir: Directory to save output metrics.
    :type output_dir: str
    :param custom_model_name: Custom model name or path.
    :type custom_model_name: str
    :param custom_num_nodes: Custom number of nodes.
    :type custom_num_nodes: int
    :param custom_num_gpus: Custom number of GPUs.
    :type custom_num_gpus: int
    :param custom_tp_size: Custom tensor parallel size.
    :type custom_tp_size: int
    :param run_type: Type of run (e.g., 'a', 'b', 'c', 'd').
    :type run_type: str
    :param model_size: Size of the model (e.g., 'small', 'medium', 'large').
    :type model_size: str
    :param custom_descriptor: Custom descriptor for the run.
    :type custom_descriptor: str
    :returns: None
    :rtype: None
    """

    with open(config_file, "r") as f:
        base_config = yaml.safe_load(f)

        for input_batch_toggle in [True]:

            # Update config with run parameters
            config = update_config(
                base_config=base_config,
                input_batch_toggle=input_batch_toggle,
                custom_model_name=custom_model_name,
                custom_num_nodes=custom_num_nodes,
                custom_num_gpus=custom_num_gpus,
                custom_tp_size=custom_tp_size,
                run_type=run_type,
            )
            descriptor = (
                custom_descriptor + "\nSingle Input Inference"
                if input_batch_toggle == False
                else custom_descriptor + "\nBatch Input Inference"
            )

            # Create ML inference workflow
            input_queue = Queue()
            response_queue = Queue()

            num_nodes = custom_num_nodes
            offset = config["hardware"].get("node_offset", 0)

            rp_event = Event()

            print(f"Initializing pipeline for {model_size} model", flush=True)

            config = InferenceConfig.from_dict(config)
            pipeline = Inference(config, input_queue)
            pipeline.initialize()
            print(
                f"Pipeline initialized successfully for {model_size} model!\n",
                flush=True,
            )
            # Generate arbitrary prompts of fixed length from William Shakespeare's Sonnet dataset.
            samples = SonnetDataset(
                dataset_path="inference/sonnet.dataset",
                random_seed=42,
                model_name_or_path=config.model.model_name,
                num_requests=num_prompts,
                input_len_tokens=128,
                prefix_len_tokens=48,
                return_prompt_formatted=False,
            ).sample()

            max_batch_len = config.batching.max_batch_size
            model_name = config.model.model_name
            # Spin up read-worker to read responses from inf-worker.
            excel_workbook = os.path.join(output_dir, f"{model_size}.xlsx")
            sheet_name = f"dragon_type={run_type}_batch={input_batch_toggle}"
            reader_end_ev = Event()
            reader = MetricsConsolidator(
                response_queue,
                reader_end_ev,
                rp_event,
                descriptor,
                time.time(),
                excel_workbook,
                sheet_name,
            )
            reader_proc = Process(
                target=reader.read_and_compute, args=(num_prompts,)
            )

            reader_proc.start()
            print(
                f"Reader process started successfully for {model_size} model!\n",
                flush=True,
            )
            times = []
            start_time = time.time()
            if input_batch_toggle == False:
                for prompt in samples:
                    pipeline.query(([prompt], response_queue))
                rp_event.set()
            else:
                for i in range(0, len(samples), max_batch_len):
                    prompt_batch = samples[i : i + max_batch_len]
                    print(
                        f"Sending batch of size {len(prompt_batch)} to inference pipeline.",
                        flush=True,
                    )
                    pipeline.query((prompt_batch, response_queue))

                rp_event.set()

            # Wait here until all responses are processed by the read-worker.
            while not reader_end_ev.is_set():
                continue
            end_time = time.time()
            print(
                f"Total time for {num_prompts} prompts = {end_time - start_time} seconds."
            )

            # Close ML inference workflow and reader.
            pipeline.destroy()
            reader_proc.join()
            response_queue.close()

            time.sleep(5)
            print(f"Process completed successfully!\n\n\n")


def main(config_file, model_size, run_type, num_prompts, output_dir):
    """Main function to run performance tests for different model sizes and configurations.

    :param config_file: Path to the configuration file.
    :type config_file: str
    :param model_size: Size of the model (e.g., 'small', 'medium', 'large').
    :type model_size: str
    :param run_type: Type of run (e.g., 'a', 'b', 'c', 'd').
    :type run_type: str
    :param num_prompts: Number of prompts to send to the inference pipeline.
    :type num_prompts: int
    :param output_dir: Directory to save output metrics.
    :type output_dir: str
    :returns: None
    :rtype: None
    """
    if model_size == "small":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/hub/models--meta-llama--Llama-3.1-8B/snapshots/d04e592bb4f6aa9cfee91e2e20afa771667e1d4b"
        if run_type == "a":
            # Single Node & Single GPU - Smallest instance of model.
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=1,  # 1 Node
                custom_num_gpus=1,  # 1 GPU
                custom_tp_size=1,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Single Node & Single GPU with tp_size=1.",
            )

        elif run_type == "b":
            # Single Node & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=1,  # 1 Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=1,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Single Node & Multi GPU with tp_size=1.",
            )

        elif run_type == "c":
            # 2 Nodes & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=2,  # 2 Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=1,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="2 Nodes & Multi GPU with tp_size=1.",
            )

        elif run_type == "d":
            # Multi Node & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=-1,  # Multi Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=1,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Multi Node & Multi GPU with tp_size=1.",
            )

    elif model_size == "medium":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/llama31_70b/transformers/models--meta-llama--Llama-3.1-70B/snapshots/349b2ddb53ce8f2849a6c168a81980ab25258dac"
        if run_type == "a":
            # Single Node & 2 GPUs - Smallest instance of model.
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=1,  # Single Node
                custom_num_gpus=2,  # 2 GPUs
                custom_tp_size=2,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Single Node & 2 GPUs with tp_size=2.",
            )

        elif run_type == "b":
            # Single Node & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=1,  # Single Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=2,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Single Node & Multi GPU with tp_size=2.",
            )

        elif run_type == "c":
            # 2 Nodes & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=2,  # Multi Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=2,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="2 Nodes & Multi GPU with tp_size=2.",
            )

        elif run_type == "d":
            # Multi Node & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=-1,  # Multi Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=2,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Multi Node & Multi GPU with tp_size=2.",
            )

    elif model_size == "large":
        custom_model_name = "/lus/scratch/kalantzi/huggingface/llama31_405b/transformers/models--hugging-quants--Meta-Llama-3.1-405B-Instruct-GPTQ-INT4/snapshots/ba9e738385d74a19443cc38235a8f647b208d938"
        if run_type == "a":
            # Single Node & 4 GPUs - Smallest instance of model.
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=1,  # Single Node
                custom_num_gpus=4,  # 4 GPUs
                custom_tp_size=4,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Single Node & 4 GPUs with tp_size=4.",
            )

        elif run_type == "b":
            # Single Node & Multi GPUs
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=1,  # Single Node
                custom_num_gpus=-1,  # 4 GPUs
                custom_tp_size=4,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Single Node & Multi GPU with tp_size=4.",
            )

        elif run_type == "c":
            # 2 Nodes & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=2,  # Multi Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=4,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="2 Nodes & Multi GPU with tp_size=4.",
            )

        elif run_type == "d":
            # Multi Node & Multi GPU
            run(
                config_file=config_file,
                num_prompts=num_prompts,
                output_dir=output_dir,
                custom_model_name=custom_model_name,
                custom_num_nodes=-1,  # Multi Node
                custom_num_gpus=-1,  # Multi GPU
                custom_tp_size=4,
                run_type=run_type,
                model_size=model_size,
                custom_descriptor="Multi Node & Multi GPU with tp_size=4.",
            )


if __name__ == "__main__":
    # mp.set_start_method("dragon")

    parser = ArgumentParser()
    parser.add_argument(
        "--config_file",
        required=True,
        type=str,
        help="The config.yaml with workflow specific input metadata.",
    )
    parser.add_argument(
        "--model_size",
        required=True,
        type=str,
        choices=["small", "medium", "large"],
        help="Determines model size you want to capture performance metrics for. Small=Llama3.1 8B; Medium=Llama3.1 70B; Large=Llama3.1 405B",
    )
    parser.add_argument(
        "--num_prompts",
        required=True,
        type=int,
        help="Determines the total number of prompts that will be sent to the inference pipeline.",
    )
    parser.add_argument(
        "--run_type",
        required=True,
        type=str,
        choices=["a", "b", "c", "d"],
        help="Determines run type. 'a'=Single Node & Single/Smallest GPU instance; 'b'=Single Node & Multi GPU; 'c'=2 Nodes & Multi GPU; 'd'=Multi Node & Multi GPU;",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        type=str,
        help="Provice the directory path where you want to save the csv files that get generated for each run.",
    )
    args = parser.parse_args()

    main(
        args.config_file,
        args.model_size,
        args.run_type,
        args.num_prompts,
        args.output_dir,
    )
