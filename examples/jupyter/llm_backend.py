import dragon
import multiprocessing as mp
from dragon.globalservices.node import get_list, query
from transformers import BlenderbotTokenizer, BlenderbotForConditionalGeneration
import torch
from py3nvml.py3nvml import *
import time
import os
import queue
import socket


def get_device_count():
    """Uses py3nvml to get the number of gpus on a node.

    :return: the number of devices on a single node
    :rtype: int
    """
    if torch.cuda.is_available(): 
        nvmlInit()
        device_count = nvmlDeviceGetCount()
        nvmlShutdown()
    else: 
        device_count=0
    return device_count


def build_device_queues():
    """Builds a dictionary of device queues.

    :return: a dict of dragon queues
    :rtype: dict
    """
    node_dict = {}
    num_devices = get_device_count()
    print(f"num_devices = {num_devices}", flush=True)
    node_list = get_list()
    for node in node_list:
        node_dict[node] = mp.Queue()
    for node in node_list:
        for device in range(num_devices):
            node_dict[node].put(device)
    return node_dict


def get_huid():
    """Gets huid for a worker's node.

    :return: returns h_uid
    :rtype: int
    """
    name = os.uname().nodename
    desc = query(str(name))
    return desc.h_uid


def get_device(device_queue):
    # grabs an unoccupied device from the nodes unique queue
    huid = get_huid()
    if torch.cuda.is_available():
        try:
            available_cuda_device = device_queue[huid].get(timeout=10)
            gpu_available = True
        except queue.Empty:
            gpu_available = False
        if gpu_available:
            device = torch.device("cuda", available_cuda_device)
        else:
            # if we don't have a device that is free, we use the cpu
            device = torch.device("cpu")
    else:
        # if we don't have a device that is free, we use the cpu
        device = torch.device("cpu")
    return device


def start_model():
    """Initializes the model and tokenizer.

    :return: returns the model and tokenizer
    :rtype: tuple
    """
    mname = "facebook/blenderbot-400M-distill"
    model = BlenderbotForConditionalGeneration.from_pretrained(mname)
    tokenizer = BlenderbotTokenizer.from_pretrained(mname)
    return model, tokenizer


class ReadWorker:
    """Handles reading from the response queue and returning the response to the front end."""

    def __init__(self, q, end_ev):
        self.q = q
        self.end_ev = end_ev

    def read(self, idx):
        """The main function for returning responses to the prompter.
        Worker idx sits inside the while loop and continuously reads from a response queue. The worker
        exits when the queue is empty and the end_ev is set.

        :param idx: worker id
        :type idx: int
        """
        print(f"read_worker {idx} started", flush=True)
        # sit in this while loop and wait for work to read
        while True:
            try:
                # gets the prompt from the queue
                response_info = self.q.get(timeout=1)
                # parses response and prompt id pairs
                prompt = response_info[0]
                prompt_id = response_info[1]
                response = response_info[2]
                responder_id = response_info[3]
                # this print could be replaced by passing the response back to the prompt_id if there are many prompters
                print(
                    f" {prompt_id} asked: {prompt} \n worker {responder_id} responded: {response} \n read worker {idx} handled this response",
                    flush=True,
                )
            except queue.Empty:
                if self.end_ev.is_set():
                    # if the queue is empty and the end event is set then we shut down
                    print(f"Shutting down read worker {idx} ", flush=True)
                    break
                else:
                    time.sleep(1)
            except Exception as e:
                print(f"Exception caught: {e}", flush=True)


class InfWorker:
    """The main inference worker."""

    def __init__(self, q_in, q_out, device_queue, end_ev):
        self.q_in = q_in
        self.q_out = q_out
        self.end_ev = end_ev
        self.device_queue = device_queue

    def infer(self, idx):
        """Inference worker function. Worker idx gets a device, initializes the model, and places the model on
        that device. It then enters a while loop that continually checks a shared dragon queue that contains the prompt
        with the prompter's ID. It tokenizes the prompt, places the prompt on the device, and then generates a response.
        It places this response in the shared response queue. The worker exits once both the prompt queue is empty and
        the end event is set.

        :param idx: inference worker id
        :type idx: int
        """
        print(f"inf_worker {idx} started", flush=True)
        device = get_device(self.device_queue)
        # starting the model and tokenizer in the worker avoids pickling and keeps start up costs equal
        model, tokenizer = start_model()
        model.to(device)
        print(f" worker {idx} has device {device} on {socket.gethostname()}", flush=True)
        # sit in this while loop and wait for work
        while True:
            try:
                # gets the prompt from the queue
                prompt_id_pair = self.q_in.get(timeout=1)
                # parses id and prompt pair
                prompt = prompt_id_pair[0]
                id = prompt_id_pair[1]
                # generates reply from the model
                reply = self._respond(prompt, model, tokenizer, device)
                # removes some special characters.
                reply = str(reply[0]).strip("<s>")
                reply = reply.strip("</s>")
                self.q_out.put((prompt, id, reply, idx))
            except queue.Empty:
                if self.end_ev.is_set():
                    # if the queue is empty and the end event is set then we shut down
                    print(f"Shutting down inference worker {idx} ", flush=True)
                    break
                else:
                    time.sleep(1)
            except Exception as e:
                print(f"Exception caught: {e}", flush=True)

    def _respond(self, prompt, model, tokenizer, device):
        """Generates the response.

        :param prompt: input prompt
        :type prompt: str
        :param model: language model
        :type model: transformers.modeling_utils.PreTrainedModel
        :param tokenizer: tokenizer
        :type tokenizer: transformers.tokenization_utils_base.PreTrainedTokenizerBase
        :param device: device where response is generated
        :type device: torch.device
        :return: response
        :rtype: str
        """
        print("reading prompt", flush=True)
        input_ids = tokenizer([prompt], return_tensors="pt")
        input_ids = input_ids.to(device)
        output = model.generate(**input_ids, min_new_tokens=100, max_new_tokens=300)

        # Decode the generated text and return it
        reply_ids = tokenizer.batch_decode(output)
        return reply_ids


class InfWorkers:
    """Orchestrates the inference and response workers."""

    def __init__(self, num_workers, num_readers):
        self.num_workers = num_workers
        self.num_readers = num_readers
        # handles device allocation on node
        self.device_queue = build_device_queues()
        # event to signal we're done with the chat
        self.end_ev = mp.Event()
        # shared queue where we place prompts
        self.q_in = mp.Queue()
        self.q_out = mp.Queue()
        # pool and base inference worker that does inference work on prompt
        self.inf_pool = mp.Pool(self.num_workers)
        self.worker = InfWorker(self.q_in, self.q_out, self.device_queue, self.end_ev)
        # pool and base read worker that handles reading and eventually routing of replys
        self.read_pool = mp.Pool(self.num_readers)
        self.reader = ReadWorker(self.q_out, self.end_ev)

    def start(self):
        """Asychronously launches the inference and read workers."""
        print(f"starting inf pool with {self.num_workers} and read pool with {self.num_readers}", flush=True)
        # list to get reasonable placement of workers and keep track of who is responding
        worker_list = [i for i in range(self.num_workers)]
        # actually launch job. Use async so we return and can read prompts
        print(f"worker list = {worker_list}", flush=True)
        workers = self.inf_pool.map_async(self.worker.infer, worker_list, 1)
        reader_list = [i for i in range(self.num_readers)]
        # actually launch job. Use async so we return and can read prompts
        print(f"reader list = {reader_list}", flush=True)
        readers = self.read_pool.map_async(self.reader.read, reader_list, 1)

    def close(self):
        """Sets the end event and then closes and joines the inference and response worker pool."""
        # sets the end event and closes everything
        print(f"closing everything", flush=True)
        self.end_ev.set()
        self.inf_pool.close()
        self.inf_pool.join()
        self.read_pool.close()
        self.read_pool.join()
        self.q_in.close()
        self.q_out.close()

    def prompt(self, prompt):
        """Accepts a prompt and then couples that prompt with an id before placing in the shared input queue.

        :param prompt: user provided response
        :type prompt: str
        """
        # place prompt with h_uid in queue
        huid = get_huid()
        print(f"putting a prompt in the queue from {huid}", flush=True)
        self.q_in.put((prompt, huid))
