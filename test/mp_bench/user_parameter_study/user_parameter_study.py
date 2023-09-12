import multiprocessing as mp
import time
import subprocess
import pickle
import datetime
import json

class ParameterStudy:
    def __init__(self, num_dimensions=1, executable=None, func=None, num_workers=4):
        if executable==None and func==None:
            raise ParameterStudy.ParameterStudyException("No function or executable passed")
        if executable != None and func != None:
            raise ParameterStudy.ParameterStudyException("Passed both a function and an executable")
        self.__num_dims = num_dimensions
        self.__executable = executable
        self.__func = func
        self.__num_workers = num_workers
        self.__parameter_space = []
        self.results = None
        self.total_time = None
        self.compute_time = None
        self.data_dict = {}

    def add_work_item(self, parameters):
        if (len(parameters) != self.__num_dims):
            fail = f'{parameters} not of length {self.__num_dims}'
            raise ParameterStudy.ParameterStudyException(fail)
        if self.__executable != None:
            parameters = list(map(str, parameters))
        self.__parameter_space.append(parameters)
   
    def shell_func(self, parameters_list):
        if self.__executable == None:
            result = self.__func(*parameters_list)
        else:
            output = subprocess.Popen(self.__executable.split()+parameters_list,stdout=subprocess.PIPE)
            result = output.communicate()[0].decode()
        return result


    #runs test returns results
    def go(self):
        mp.set_start_method('spawn')
        start_time_setup = time.time_ns()
        with mp.Pool(self.__num_workers) as the_pool:
            start_time_no_setup = time.time_ns()
            map_results = the_pool.map(self.shell_func, self.__parameter_space)
            end_time_no_setup = time.time_ns()
        end_time_setup = time.time_ns()

        self.results = list(map_results)
        self.total_time = end_time_setup - start_time_no_setup
        self.compute_time = end_time_no_setup - start_time_no_setup
    
    def performance_info(self, output='dict'):
        self.__process_data()
        if output == 'dict':
            return self.data_dict
        elif output == 'json':
            return json.dumps(self.data_dict, indent=4, default=str)
        elif output == 'str':
            return self.__string_result()
        else:
            raise ParameterStudy.ParameterStudyException('Invalid output type ("dict", "str", or "json")')

    def __dict_setup(self):
        self.data_dict['hostname'] = subprocess.check_output('hostname').decode().strip()
        self.data_dict['date'] = datetime.datetime.now()
        self.data_dict['function'] = self.__func
        self.data_dict['executable'] = self.__executable
        self.data_dict['num_workers'] = self.__num_workers
        self.data_dict['work_items'] = len(self.__parameter_space)
        self.data_dict['avg_response_size'] = len(pickle.dumps(self.results)) / self.data_dict['work_items']
        self.data_dict['avg_message_size'] = len(pickle.dumps(self.__parameter_space)) / self.data_dict['work_items']
        self.data_dict['latency'] = self.total_time / 1000
        self.data_dict['compute_time'] = self.compute_time / 1000
        self.data_dict['setup_time'] = self.data_dict['latency'] - self.data_dict['compute_time']

    def __process_data(self):
        self.__dict_setup()
        transmitted_bytes = len(pickle.dumps(self.results)) + len(pickle.dumps(self.__parameter_space))
        avg_compute_time_per_item = self.compute_time / self.data_dict['work_items']
        avg_setup_time_pw = self.data_dict['setup_time'] / self.__num_workers
        avg_bandwidth = transmitted_bytes / self.compute_time

        self.data_dict['avg_setup_time_per_worker'] = avg_setup_time_pw / 1000
        self.data_dict['avg_compute_time_per_work_item'] = avg_compute_time_per_item / 1000
        self.data_dict['avg_bandwidth'] = avg_bandwidth * (1000000000 / (2**20))

    def __string_result(self):
        return (
                f'Parameter Study Performance\n'
                f'Hostname: {self.data_dict["hostname"]}\n'
                f'Date: {self.data_dict["date"]}\n'
                f'Function: {self.data_dict["function"]}\n'
                f'Executable: {self.data_dict["executable"]}\n'
                f'Number of workers: {self.data_dict["num_workers"]}\n'
                f'Number of work items: {self.data_dict["work_items"]}\n'
                f'Average message size (bytes): {self.data_dict["avg_message_size"]}\n'
                f'Average response size (bytes): {self.data_dict["avg_response_size"]}\n'
                f'Latency (\xb5s): {self.data_dict["latency"]}\n'
                f'Compute time (\xb5s): {self.data_dict["compute_time"]}\n'
                f'Setup time (\xb5s): {self.data_dict["setup_time"]}\n'
                f'Average setup time per worker (\xb5s): {self.data_dict["avg_setup_time_per_worker"]}\n'
                f'Average compute time per work item (\xb5s): {self.data_dict["avg_compute_time_per_work_item"]}\n'
                f'Average bandwidth (mB/s): {self.data_dict["avg_bandwidth"]}'
               )

    class ParameterStudyException(Exception):
        pass
