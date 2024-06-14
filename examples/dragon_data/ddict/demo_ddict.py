import dragon
from dragon.data.ddict.ddict import DDict
import multiprocessing as mp

def client_function(d, client_id):

    key1 = 'hello' + str(client_id)
    d[key1] = 'world' + str(client_id)
    print(f'added {key1} to dictionary')

def main():
    """
    Test put and get functions.
    """
    # bring up dictionary
    # number of manager = 2
    # number of nodes = 1
    # total size of dictionary = 2000000
    # clients and managers will be on different node in round-robin fashion
    d = DDict(2,1,2000000)
    # create 10 clients, each of them implements client_function 
    procs = []
    for i in range(10):
        proc = mp.Process(target=client_function, args=(d, i))
        procs.append(proc)
        proc.start()
    
    # waiting for all client process to finish
    for i in range(10):
        procs[i].join()
    # lookup all keys and vals
    try:
        for i in range(10):
            key = 'hello' + str(i)
            val = d[key]
            print(f'd[{repr(key)}] = {repr(val)}')
            # print out all key and value given the key
            assert val == 'world' + str(i)
    except Exception as e:
        print(f'Got exception {repr(e)}')
    # destroy dictionary
    d.destroy()

if __name__ == "__main__":
    main()
