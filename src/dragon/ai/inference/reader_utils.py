import dragon
import multiprocessing as mp
from queue import Empty
import time
import pandas as pd
import os


class ReadWorker:
    """
    Handles reading from the response queue and returning the response to the front end.
    Also terminates reading as soon as the expected number of inputs have been received.
    """

    def __init__(self, q, end_ev):
        self.q = q
        self.end_ev = end_ev

    def read(self, num_prompts):
        """Return responses to the prompter until all prompts are read.

        The worker sits inside a loop and continuously reads from a response
        queue. It exits when the expected number of prompts has been read and
        the end event is set.

        :param num_prompts: Number of expected input prompts to read.
        :type num_prompts: int
        """
        # sit in this while loop and wait for work to read
        counter = 0
        while True:
            try:
                response_info = self.q.get(timeout=0.1)
                response_info["counter"] = str(counter + 1)
                counter += 1
            except Empty:
                if counter == num_prompts:
                    # if the queue is empty and the end event is set then we shut down
                    self.end_ev.set()
                    break
                else:
                    pass
            except Exception as e:
                print(f"Exception caught: {e}", flush=True)


class MetricsConsolidator:
    """Handles reading from the response queue and returning the response to the front end.
    Also terminates reading as soon as the expected number of inputs have been received.
    """

    def __init__(
        self,
        q,
        end_ev,
        read_ev,
        descriptor,
        base_start_time,
        excel_workbook,
        sheet_name,
    ):
        """Initialize a :class:`MetricsConsolidator` instance.

        :param q: Multiprocessing queue to read metrics from.
        :type q: mp.Queue
        :param end_ev: Multiprocessing event to signal the end of reading.
        :type end_ev: mp.Event
        :param read_ev: Event that signals when all metrics have been read.
        :type read_ev: mp.Event
        :param descriptor: Descriptor string used to identify the metrics.
        :type descriptor: str
        :param base_start_time: Base start time used to compute total
            processing time.
        :type base_start_time: float
        :param excel_workbook: Path to the Excel workbook to write metrics to.
        :type excel_workbook: str
        :param sheet_name: Sheet name in the Excel workbook for metrics.
        :type sheet_name: str
        """
        self.q = q
        self.end_ev = end_ev
        self.read_ev = read_ev
        self.descriptor = descriptor
        self.base_start_time = base_start_time
        self.excel_workbook = excel_workbook
        self.sheet_name = sheet_name

    def read_and_compute(self, num_prompts):
        """Read metrics from the queue and compute aggregated statistics.

        The worker sits inside a loop and continuously reads from a metrics
        queue. It exits when the expected number of prompts has been read and
        the end event is set.

        :param num_prompts: Number of expected input prompts to read.
        :type num_prompts: int
        """
        counter = 1

        hostname = []
        inf_worker_id = []
        devices = []
        total_time = []
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
                total_time.append(time.time() - self.base_start_time)
                batch_size.append(response_info["batch_size"])

                # Latency
                cpu_head_network_latency.append(response_info["cpu_head_network_latency"])
                guardrails_inference_latency.append(response_info["guardrails_inference_latency"])
                guardrails_network_latency.append(response_info["guardrails_network_latency"])
                model_inference_latency.append(response_info["model_inference_latency"])
                model_network_latency.append(response_info["model_network_latency"])
                end_to_end_latency.append(response_info["end_to_end_latency"])

                # Throughput
                requests_per_second.append(response_info["requests_per_second"])
                total_tokens_per_second.append(response_info["total_tokens_per_second"])
                total_output_tokens_per_second.append(response_info["total_output_tokens_per_second"])

                counter += 1
            except Empty:
                if (
                    counter == num_prompts + 1 and self.read_ev.is_set()
                ):  # if the queue is empty and the end event is set then we shut down
                    self.end_ev.set()
                    self.total_proc_time = round(time.time() - self.base_start_time)
                    print(f"Total processing time: {self.total_proc_time}", flush=True)
                    break
                else:
                    pass
            except Exception as e:
                print(f"METRICS CONSOLIDATOR Exception caught: {e}", flush=True)

        # Create pandas dataframe
        data = {
            # Identifiers
            "hostname": hostname,
            "inf_worker_id": inf_worker_id,
            "devices": devices,
            "total_inference_time": total_time,
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
        print(f"\n{df}\n", flush=True)

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
        if os.path.exists(self.excel_workbook):  # Workbook already exists. Create sheet only
            with pd.ExcelWriter(
                self.excel_workbook,
                engine="openpyxl",
                mode="a",
                if_sheet_exists="replace",
            ) as writer:
                df.to_excel(writer, sheet_name=self.sheet_name, index=False)
        else:  # Workbook does not exist.
            df.to_excel(self.excel_workbook, sheet_name=self.sheet_name, index=False)
