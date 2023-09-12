import multiprocessing as mp
import os
import test_util
import datetime
import time
import json

class BasicParameterStudy:
    def __init__(self):
        self.args = None
        self.results = None

    def __setup(self):
        parser = test_util.add_default_args('Run a basic parameter study')
        parser = test_util.add_connections_args(parser)
        parser.add_argument('--response_size', type=int, default=4, 
                            help='size of the message to pass back')
        parser.add_argument('--work_items', type=int, default=8,
                            help='number of work items in the parameter study')
        self.args = test_util.process_args(parser)
        mp.set_start_method('spawn')
        
    def run(self):
        self.__setup()
        times = test_util.iterations(self.do_parameter_study, self.args)
        self.results = ParameterResults(self.args, times)
        if self.args.json:
            return self.results.dump()
        return self.results

    def dummy_func(self, x):
        return bytes(self.args.response_size)

    def generate_work_items(self):
        for item in range(self.args.work_items):
            yield bytes(self.args.message_size)

    def do_parameter_study(self):
        inputlist = [bytes(self.args.message_size) for i in range(self.args.work_items)]
        
        start_time_setup = time.time_ns()
        with mp.Pool(self.args.num_workers) as the_pool:
            start_time_no_setup = time.time_ns()
            the_pool.map(self.dummy_func, inputlist)
            end_time_no_setup = time.time_ns()
        end_time_setup = time.time_ns()

        return (end_time_setup - start_time_setup, end_time_no_setup - start_time_no_setup)

class ParameterResults():
    def __init__(self, args, times):
        self.args = args
        self.data_dict = {}
        self.times = times
        self.data_dict['num_workers'] = args.num_workers
        self.data_dict['response_size'] = args.response_size
        self.data_dict['message_size'] = args.message_size
        self.data_dict['work_items'] = args.work_items
        self.data_dict['hostname'] = test_util.get_hostname().strip()
        self.data_dict['date'] = datetime.datetime.now()
        self.__process_data()

    def __process_data(self):
        sum_size = (self.args.message_size + self.args.response_size) * self.args.work_items
        avg_latency = 0
        avg_compute_time=0
        for time in self.times:
            avg_latency = avg_latency + time[0]
            avg_compute_time = avg_compute_time + time[1]
        avg_latency = avg_latency / len(self.times)
        avg_compute_time = avg_compute_time / len(self.times)
        avg_setup_time = avg_latency - avg_compute_time
        avg_setup_time_pw = avg_setup_time / self.args.num_workers
        avg_compute_time_pwi = avg_compute_time / self.args.work_items
        avg_bandwidth = sum_size / avg_compute_time

        self.data_dict['avg_latency'] = avg_latency / 1000
        self.data_dict['avg_compute_time'] = avg_compute_time / 1000
        self.data_dict['avg_setup_time'] = avg_setup_time / 1000
        self.data_dict['avg_setup_time_per_worker'] = avg_setup_time_pw / 1000
        self.data_dict['avg_compute_time_per_work_item'] = avg_compute_time_pwi / 1000
        self.data_dict['avg_bandwidth'] = avg_bandwidth * (1000000000 / (2**20))

    def __str__(self):
        return (
                f'Test: Basic Parameter Study\n'
                f'Hostname: {self.data_dict["hostname"]}\n'
                f'Date: {self.data_dict["date"]}\n'
                f'Number of workers: {self.data_dict["num_workers"]}\n'
                f'Number of work items: {self.data_dict["work_items"]}\n'
                f'Message size (bytes): {self.data_dict["message_size"]}\n'
                f'Response size (bytes): {self.data_dict["response_size"]}\n'
                f'Average latency (\xb5s): {self.data_dict["avg_latency"]}\n'
                f'Average compute time (\xb5s): {self.data_dict["avg_compute_time"]}\n'
                f'Average setup time (\xb5s): {self.data_dict["avg_setup_time"]}\n'
                f'Average setup time per worker (\xb5s): {self.data_dict["avg_setup_time_per_worker"]}\n'
                f'Average compute time per work item (\xb5s): {self.data_dict["avg_compute_time_per_work_item"]}\n'
                f'Average bandwidth (mB/s): {self.data_dict["avg_bandwidth"]}'
               )
    def dump(self):
        return json.dumps(self.data_dict, indent=4, default=str)

    def python_dict(self):
        return self.data_dict



if __name__ == '__main__':
    test = BasicParameterStudy()
    print(test.run())
