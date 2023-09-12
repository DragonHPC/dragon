import subprocess
import resource
import json
import csv
import os
import datetime
import argparse
import sys
import multiprocessing

class TestAutomation:
    def __init__(self, open_file_desc_limit):
        self.open_file_desc_limit = open_file_desc_limit
        self.directory_name = ('results'+ 
                              str((subprocess.check_output('hostname').decode()).strip())+ 
                              str(datetime.datetime.now()))
        self.cpus = multiprocessing.cpu_count()
        self.args = None
        self.__setup()

    def __setup(self):
        os.mkdir(self.directory_name)
        self.args = self.__get_args()
        if (not len(sys.argv) > 1) or ((len(sys.argv) == 3) and (self.args.num_iterations is not None)):
            self.run_all_tests()
        else:
            self.__run_subset_tests()

    def __get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--basic_process_test', action='store_true',
                            help='run suite of benchmarks for basic_process_test.py')
        parser.add_argument('--basic_pool_test', action='store_true',
                            help='run suite of benchmarks for basic_pool_test.py')
        parser.add_argument('--pool_invocation_test', action='store_true',
                            help='run suite of benchmarks for pool_invocation_test.py')      
        parser.add_argument('--send_receive_test', action='store_true',
                            help='run suite of benchmarks for send_receive_test.py')
        parser.add_argument('--chain_token_test', action='store_true',
                            help='run suite of benchmarks for chain_token_test.py')
        parser.add_argument('--chain_token_pipe_test', action='store_true',
                            help='run suite of benchmarks for chain_token_pipe_test.py')
        parser.add_argument('--chain_token_test_simple', action='store_true',
                            help='run suite of benchmarks for chain_token_test_simple.py')
        parser.add_argument('--basic_managed_dict_test', action='store_true',
                            help='run suite of benchmarks for basic_managed_dict_test.py')
        parser.add_argument('--access_managed_dict_test', action='store_true',
                            help='run suite of benchmarks for access_managed_dict_test.py') 
        parser.add_argument('--num_iterations', type=int, default=None,
                            help='number of iteration for each instance of run tests')
        parser.add_argument('--spins', type=int, default=None,
                            help='number of spins for each instance of a test with spins.'+
                            ' can lead to duplicated tests.')
        parser.add_argument('--max_workers', type=int, default=self.cpus,
                            help='maximum number of workers a test will run with')
        return parser.parse_args() 
        
        
    def run_all_tests(self):
        self.run_basic_pool_test()
        self.run_basic_process_test()
        self.run_pool_invocation_test()
        self.run_send_receive_test()
        self.run_chain_token_test()
        self.run_chain_token_pipe_test()
        self.run_chain_token_test_simple()
        self.run_basic_managed_dict_test()
        self.run_access_managed_dict_test()

    def __run_subset_tests(self):
        test_run = False
        if self.args.basic_pool_test:
            self.run_basic_pool_test()
            test_run = True
        if self.args.basic_process_test:
            self.run_basic_process_test()
            test_run = True
        if self.args.pool_invocation_test:
            self.run_pool_invocation_test()
            test_run = True
        if self.args.send_receive_test:
            self.run_send_receive_test()
            test_run = True
        if self.args.chain_token_test:
            self.run_chain_token_test()
            test_run = True
        if self.args.chain_token_pipe_test:
            self.run_chain_token_pipe_test()
            test_run = True
        if self.args.chain_token_test_simple:
            self.run_chain_token_test_simple()
            test_run = True
        if self.args.basic_managed_dict_test:
            self.run_basic_managed_dict_test()
            test_run = True
        if self.args.access_managed_dict_test:
            self.run_access_managed_dict_test()
            test_run = True
        if not test_run:
            self.run_all_tests()

    def run_basic_pool_test(self):
        input_dict = self.__process_csv_input('automation_input/basic_pool_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/basic_pool_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/basic_pool_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/basic_pool_test_output.csv')

    def run_basic_process_test(self):
        input_dict = self.__process_csv_input('automation_input/basic_process_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/basic_process_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/basic_process_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/basic_process_test_output.csv')

    def run_pool_invocation_test(self):
        input_dict = self.__process_csv_input('automation_input/pool_invocation_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/pool_invocation_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/pool_invocation_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/pool_invocation_test_output.csv')

    def run_send_receive_test(self):
        input_dict = self.__process_csv_input('automation_input/send_receive_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/send_receive_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--message_size', input_dict['message_size'][i],
                                                       '--response_size', input_dict['response_size'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/send_receive_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/send_receive_test_output.csv')

    def run_chain_token_test(self):
        input_dict = self.__process_csv_input('automation_input/chain_token_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/chain_token_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--message_size', input_dict['message_size'][i],
                                                       '--spins', input_dict['spins'][i],
                                                       '--num_tokens', input_dict['num_tokens'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/chain_token_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/chain_token_test_output.csv')

    def run_chain_token_pipe_test(self):
        input_dict = self.__process_csv_input('automation_input/chain_token_pipe_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/chain_token_pipe_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--message_size', input_dict['message_size'][i],
                                                       '--spins', input_dict['spins'][i],
                                                       '--num_tokens', input_dict['num_tokens'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/chain_token_pipe_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/chain_token_pipe_test_output.csv')  

    def run_chain_token_test_simple(self):
        input_dict = self.__process_csv_input('automation_input/chain_token_test_simple_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/chain_token_test_simple.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--message_size', input_dict['message_size'][i],
                                                       '--spins', input_dict['spins'][i],
                                                       '--num_tokens', input_dict['num_tokens'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/chain_token_test_simple.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/chain_token_test_simple_output.csv')

    def run_basic_managed_dict_test(self):
        input_dict = self.__process_csv_input('automation_input/basic_managed_dict_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/basic_managed_dict_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--message_size', input_dict['message_size'][i],
                                                       '--spins', input_dict['spins'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/basic_managed_dict_test.py',
                                               '--num_workers', str(self.args.max_workers),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())       
        self.__process_json(output, self.directory_name+'/basic_managed_dict_test_output.csv')

    def run_access_managed_dict_test(self):
        input_dict = self.__process_csv_input('automation_input/access_managed_dict_test_input.csv')
        output = []
        for i in range(len(input_dict['num_workers'])):
            if int(input_dict['num_workers'][i]) <= self.args.max_workers:
                output.append(subprocess.check_output([sys.executable, 'tests/access_managed_dict_test.py',
                                                       '--num_workers', input_dict['num_workers'][i],
                                                       '--num_iterations', input_dict['num_iterations'][i],
                                                       '--message_size', input_dict['message_size'][i],
                                                       '--spins', input_dict['spins'][i],
                                                       '--json'
                                                     ]).decode())
        output.append(subprocess.check_output([sys.executable, 'tests/access_managed_dict_test.py',
                                               '--num_workers', str(int(self.args.max_workers/2)),
                                               '--num_iterations', '5',
                                               '--json'
                                             ]).decode())
        self.__process_json(output, self.directory_name+'/access_managed_dict_test_output.csv')

    def __process_csv_input(self, file_name):
        with open(file_name, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            input_dict = {}
            first = True
            for row in reader:
                if first:
                    for key in row:
                        if (key == 'num_iterations') and (self.args.num_iterations is not None):
                            input_dict[key] = [str(self.args.num_iterations)]
                        elif (key == 'spins') and (self.args.spins is not None):
                            input_dict[key] = [str(self.args.spins)]
                        else:
                            input_dict[key] = [row[key]]
                    first = False
                else:
                    for key in row:
                        if (key == 'num_iterations') and (self.args.num_iterations is not None):
                            input_dict[key].append(str(self.args.num_iterations))
                        elif (key == 'spins') and (self.args.spins is not None):
                            input_dict[key].append(str(self.args.spins))
                        else:
                            input_dict[key].append(row[key])
            return input_dict


    def __process_json(self, json_list, file_name):
        first = json.loads(json_list[0])
        headers = []
        for key in first:
            headers.append(key)

        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for d in json_list:
                writer.writerow(json.loads(d))


if __name__ == '__main__':
    test = TestAutomation(1024)



