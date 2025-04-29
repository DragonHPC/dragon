"""This file has all the functions used to gather telemetry data on each node. It relies heavily on py3nvml to gather this data. 
"""

from py3nvml.py3nvml import *
import py3nvml.nvidia_smi as smi
from tqdm import tqdm
import os
import sys
import socket
import time


def call_nvml():
    """Gathers telemetry data for Nvidia GPUs and the CPU.

    :return: telemetry information for all devices on a node
    :rtype: list of lists
    """
    nvmlInit()
    load1, load5, load15 = os.getloadavg()
    deviceCount = nvmlDeviceGetCount()
    gpu_info_list = []
    name = socket.gethostname()
    for i in range(deviceCount):
        handle = nvmlDeviceGetHandleByIndex(i)
        utilization = nvmlDeviceGetUtilizationRates(handle)
        uuid = nvmlDeviceGetUUID(handle)
        gpu_info_list.append([name, i, uuid, utilization.gpu, utilization.memory, load1])
    nvmlShutdown()
    return gpu_info_list


def getDeviceCount():
    """Uses py3nvml to get the number of Nvidia GPUs on a node

    :return: number of devices on a node
    :rtype: int
    """
    nvmlInit()
    deviceCount = nvmlDeviceGetCount()
    nvmlShutdown()
    return deviceCount


def buildTelemDict(hostname, tqdm_dict, deviceID=0):
    """Initializes tqdm bars so that they print in a somewhat nice fashion

    :param hostname: hostname of node
    :type hostname: str
    :param tqdm_dict: dictionary that is initially empty and populated with tqdm bars
    :type tqdm_dict: dict
    :param deviceID: device number that we want to print. If set to None then it will print all, defaults to 0
    :type deviceID: int, optional
    """
    offset = 0
    num_devices = 2
    if deviceID is None:
        num_gpus = getDeviceCount()
        num_devices = num_gpus
        num_devices += 1
    counter = len(tqdm_dict)
    tqdm_dict[hostname] = {}
    location = offset + counter * num_devices
    bar = tqdm(total=100, position=location, file=sys.stdout)
    bar.set_description_str(f"{hostname} cpu load avg.")
    tqdm_dict[hostname]["cpu"] = bar
    if deviceID is not None:
        location = offset + counter * num_devices + 1
        bar = tqdm(total=100, position=location, file=sys.stdout)
        bar.set_description_str(f"{hostname} device {deviceID} util")
        tqdm_dict[hostname]["device"] = bar
    else:
        num_gpus = getDeviceCount()
        num_devices = num_gpus
        num_devices += 1
        for gpu_device in range(num_gpus):
            location = offset + counter * num_devices + 1 + gpu_device
            bar = tqdm(total=100, position=location, file=sys.stdout)
            bar.set_description_str(f"{hostname} device {gpu_device} util")
            tqdm_dict[hostname]["device" + str(gpu_device)] = bar
    sys.stdout.flush()


def updateTelemDict(results_telem, tqdm_dict, deviceID=0):
    """Updates the information stored in the tqdm bars

    :param results_telem: telemetry information for all devices on a node
    :type results_telem: list of lists
    :param tqdm_dict: dictionary that is populated with tqdm bars
    :type tqdm_dict: dict
    :param deviceID: device number that we want to print. If set to None then it will print all, defaults to 0
    :type deviceID: int, optional
    """
    hostname = results_telem[0][0]
    try:
        tqdm_dict[hostname]["cpu"].n = results_telem[0][5]
    except:
        buildTelemDict(hostname, tqdm_dict, deviceID=deviceID)
        tqdm_dict[hostname]["cpu"].n = results_telem[0][5]
    tqdm_dict[hostname]["cpu"].refresh()
    for gpu_info in iter(results_telem):
        device_number = gpu_info[1]
        if deviceID is not None:
            if device_number != deviceID:
                continue
            else:
                tqdm_dict[hostname]["device"].n = gpu_info[3]
                tqdm_dict[hostname]["device"].refresh()
                sys.stdout.flush()
                break
        else:
            tqdm_dict[hostname]["device" + str(device_number)].n = gpu_info[3]
            tqdm_dict[hostname]["device" + str(device_number)].refresh()
            sys.stdout.flush()


def printTelem(results_telem, deviceID=0):
    """Prints telemetry data without tqdm bars

    :param results_telem: telemetry information for all devices on a node
    :type results_telem: list of lists
    :param deviceID: device number that we want to print. If set to None then it will print all, defaults to 0
    :type deviceID: int, optional
    """
    for gpu_info in iter(results_telem):
        hostname = gpu_info[0]
        device_number = gpu_info[1]
        gpu_uuid = gpu_info[2]
        gpu_util = gpu_info[3]
        gpu_memutil = gpu_info[4]
        load1 = gpu_info[5]
        if deviceID is not None:
            if device_number != deviceID:
                continue
            else:
                print(
                    "nodename: {} cpu load average 1 minute: {} device # {} utilization: {:.2f}%".format(
                        hostname, load1, device_number, gpu_util
                    ),
                    flush=True,
                )
