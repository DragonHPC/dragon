import dragon
import argparse
import multiprocessing as mp

from dragon.data.distdictionary.dragon_dict import DragonDict

def _retrieve_value(_dict, key, client_id):
    value = _dict[key]
    print(f'Retrieving value:{value} for key:{key} for client id:{client_id} from the dictionary', flush=True)
    print(f'Closing the connection with client id:{client_id}', flush=True)
    _dict.close()
    return value

def _store_key_value(_dict, key, value, client_id):
    print(f'Storing key:{key} and value:{value} from client id:{client_id} into the dictionary', flush=True)
    _dict[key] = value
    print(f'Closing the connection with client id:{client_id}', flush=True)
    _dict.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Distributed dictionary example')
    parser.add_argument('--num_nodes', type=int, default=1,
                        help='number of nodes the dictionary distributed across')
    parser.add_argument('--managers_per_node', type=int, default=1,
                        help='number of managers per node for the dragon dict')
    parser.add_argument('--total_mem_size', type=int, default=1,
                        help='total managed memory size for dictionary in GB')

    my_args = parser.parse_args()
    mp.set_start_method("dragon")

    # Instantiate the dictionary and start the processes
    total_mem_size = my_args.total_mem_size * (1024*1024*1024)
    dd = DragonDict(my_args.managers_per_node, my_args.num_nodes, total_mem_size)

    client_proc_1 = mp.Process(target=_store_key_value, args=(dd, "Hello", "Dragon", 1))
    client_proc_1.start()
    client_proc_1.join()

    client_proc_2 = mp.Process(target=_retrieve_value, args=(dd, "Hello", 2))
    client_proc_2.start()
    client_proc_2.join()

    print(f'Done here. Stopping the Dragon Dictionary', flush=True)
    dd.stop()