import dragon
import multiprocessing as mp
from dragon.data.ddict.ddict import DDict
import os

def client_function(distdict, client_id):

    key1 = str(os.getpid()) + 'hello' + str(client_id)
    distdict[key1] = 'world' + str(client_id)
    print(f'added {key1} to dictionary')
    return key1

def main():
    mp.set_start_method('dragon')

    d = dict()

    d['Miska'] = 'Dog'
    d['Tigger'] = 'Cat'
    d[123] = 456
    d['function'] = client_function

    distdict = DDict(1,10,10000000)
    distdict['Miska'] = 'Dog'
    distdict['Tigger'] = 'Cat'
    distdict[123] = 456
    distdict['function'] = client_function

    with mp.Pool(5) as p:
        keys = p.starmap(client_function, [(distdict, client_id) for client_id in range(64)])

    print(keys)

    for key in keys:
        print(f'distdict[{repr(key)}] is mapped to {repr(distdict[key])}')

    distdict.destroy()

if __name__ == "__main__":
    main()



