import argparse
import multiprocessing
import subprocess
import json
import datetime

def get_hostname():
    return subprocess.check_output('hostname').decode()

def add_default_args(desc):
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--num_workers', type=int, default=4, 
                        help='number of workers to spawn at a time')
    parser.add_argument('--num_iterations', type=int, default=4, 
                        help='number of iterations to do')
    parser.add_argument('--burn_iterations', type=int, default=1, 
                        help='number of iterations to burn first time')
    parser.add_argument('--json', action='store_true',
                        help='output json')
    return parser

def add_connections_args(parser):
    parser.add_argument('--message_size', type=int, default=4, 
                        help='size of message to pass in')
    return parser

def add_token_args(parser):
    parser = add_connections_args(parser)
    parser.add_argument('--spins', type=int, default=100,
                        help='number of times to send the token around a chain')
    parser.add_argument('--num_tokens', type=int, default=1, 
                        help='number of tokens to pass around the ring')
    return parser

def process_args(parser):
    args = parser.parse_args()
    assert args.num_workers > 0
    assert args.num_iterations > 0
    #if not args.json:
    #    print('Testing with {} workers, {} iterations'.format(args.num_workers, args.num_iterations))
    return args

def iterations(fcn, args):
    for _ in range(args.burn_iterations):
        _ = fcn()
    
    iteration_times = []

    for _ in range(args.num_iterations):
        this_time_ns = fcn()
        iteration_times.append(this_time_ns)

    return iteration_times

class Results:
    def __init__(self, args, times, test_type):
        #self.args = args
        self.times = times
        self.num_iterations = args.num_iterations
        self.num_workers = args.num_workers
        self.times_pw = [] 
        self.avg_time = 0
        self.avg_time_pw = 0
        self.hostname = get_hostname().strip()
        self.date = datetime.datetime.now()
        self.test_type = test_type
        try:
            self.spins = args.spins
        except:
            self.spins = None
        try: 
            self.message_size = args.message_size
            self.avg_bandwidth = 0
            self.bandwidth = []
        except:
            self.message_size = None
            self.bandwidth = None
            self.avg_bandwidth = None
        try:
            self.response_size = args.response_size
        except:
            self.response_size = None
        try:
            self.num_tokens = args.num_tokens
        except:
            self.num_tokens = None
        self.times_ps = []
        self.avg_time_ps = 0
        self.__total = 0
        self.__total_pw = 0
        self.__total_ps = 0
        self.__total_bw = 0
        self.__process_data()

    def __process_data(self):
        for i in range(len(self.times)):
            self.__total = self.__total + self.times[i]
            self.times_pw.append(self.times[i] / self.num_workers)
            self.__total_pw = self.__total_pw + self.times_pw[i]
            if self.spins is not None:
                self.__process_spin_data(i)
            if self.response_size is not None:
                self.__process_size_data(i)
            self.times[i] = self.times[i] / 1000
            self.times_pw[i] = self.times_pw[i] / 1000

        self.avg_time = (self.__total / self.num_iterations) / 1000
        self.avg_time_pw = (self.__total_pw / self.num_iterations) / 1000
        self.avg_time_ps = (self.__total_ps / self.num_iterations) / 1000
        self.avg_bandwidth = (self.__total_bw / self.num_iterations) * (1000000000 / (2**20))

    def __process_spin_data(self, i):
        self.times_ps.append(self.times[i] / self.spins)
        self.__total_ps = self.__total_ps + self.times_ps[i]
        self.times_ps[i] = self.times_ps[i] / 1000
        self.bandwidth.append((self.message_size * self.num_workers * self.spins) / self.times[i])
        if self.num_tokens is not None:
            self.bandwidth[i] = self.bandwidth[i] * self.num_tokens
        self.__total_bw = self.__total_bw + self.bandwidth[i]
        self.bandwidth[i] = self.bandwidth[i] * (1000 / (2**20))

    def __process_size_data(self, i):
        self.bandwidth.append(((self.message_size + self.response_size) * self.num_workers) / self.times[i])
        self.__total_bw = self.__total_bw + self.bandwidth[i]
        self.bandwidth[i] = self.bandwidth[i] * (1000 / (2**20))

    def not__str__(self):
        if self.response_size is not None: 
            return (
                f'Test: {self.test_type}\n'
                f'Hostname: {self.hostname}\n'
                f'Date: {self.date}\n'
                f'Message size (bytes): {self.message_size}\n'
                f'Response size (bytes): {self.response_size}\n'
                f'Number of workers: {self.num_workers}\n'
                f'Average total time (\xB5s): {self.avg_time}\n'
                f'Average time per worker (\xB5s): {self.avg_time_pw}\n'
                f'Average bandwidth (MiB/s): {self.avg_bandwidth}\n'
            )

        elif self.num_tokens is not None:
            return (
                f'Test: {self.test_type}\n'
                f'Hostname: {self.hostname}\n'
                f'Date: {self.date}\n'
                f'Message size (bytes): {self.message_size}\n'
                f'Number of workers: {self.num_workers}\n'
                f'Number of spins: {self.spins}\n'
                f'Number of tokens: {self.num_tokens}\n'
                f'Average total time (\xb5s): {self.avg_time}\n'
                f'Average time per worker (\xb5s): {self.avg_time_pw}\n'
                f'Average time per spin (\xb5s): {self.avg_time_ps}\n'
                f'Average bandwidth (MiB/s): {self.avg_bandwidth}\n'
            )

        elif self.spins is not None:
            return (
                f'Test: {self.test_type}\n'
                f'Hostname: {self.hostname}\n'
                f'Date: {self.date}\n'
                f'Message size (bytes): {self.message_size}\n'
                f'Number of workers: {self.num_workers}\n'
                f'Number of spins: {self.spins}\n'
                f'Average total time (\xb5s): {self.avg_time}\n'
                f'Average time per worker (\xb5s): {self.avg_time_pw}\n'
                f'Average time per spin (\xb5s): {self.avg_time_ps}\n'
                f'Average bandwidth (MiB/s): {self.avg_bandwidth}\n'
            )             
        
        else:
            return (
                f'Test: {self.test_type}\n'
                f'Hostname: {self.hostname}\n'
                f'Date: {self.date}\n'
                f'Number of workers: {self.num_workers}\n'
                f'Average total time (\xB5s): {self.avg_time}\n'
                f'Average time per worker (\xB5s): {self.avg_time_pw}\n'
            )
    def __str__(self):
        return f'{self.avg_time}'

    def dump(self):
        data_dict = {}
        data_dict["test_type"] = self.test_type
        data_dict["hostname"] = self.hostname
        data_dict["date"] = self.date
        data_dict["num_workers"] = self.num_workers
        #data_dict["times"] = self.times
        #data_dict["times_pw"] = self.times_pw
        data_dict["avg_time"] = self.avg_time
        data_dict["avg_time_pw"] = self.avg_time_pw
        if self.spins is not None:
            data_dict["spins"] = self.spins
            #data_dict["times_ps"] = self.times_ps
            data_dict["avg_time_ps"] = self.avg_time_ps
            data_dict["message_size"] = self.message_size
            #data_dict["bandwidth"] = self.bandwidth
            data_dict["avg_bandwidth"] = self.avg_bandwidth
        if self.response_size is not None:
            data_dict["response_size"] = self.response_size
            data_dict["message_size"] = self.message_size
            data_dict["avg_bandwidth"] = self.avg_bandwidth
        if self.num_tokens is not None:
            data_dict["num_tokens"] = self.num_tokens
        return json.dumps(data_dict, indent=4, default=str)
