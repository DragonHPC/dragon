Parallel Producer - Consumer Communication with Queue
+++++++++++++++++++++++++++++++++++++++++++++++++++++

Here we show a multiple producers multiple consumers communication scheme with
Multiprocessing and Dragon.

The parent process creates and starts a number processes.  The first half acts
as a producer, creating random strings, packaging them up with a random
selection of lambdas and putting this work package into a shared queue.  The
second half of the processes acts as consumers. They get handed only the queue,
execute a blocking `get()` on it to receive the work package. Once the package
is received, they execute the lambdas one by one on the string, printing the 
output.  Finally, the parent process joins on all process objects, to ensure
they have completed successfully.

The code demonstrates the following key concepts working with Dragon:

* Shared communication objects between processes, here using a queue.
* Creating, starting and joining worker processes.
* Out-of-order execution through the output.
* Dragon's `puid`'s replace the OS process IDs (`pid`) as a unique identifier for processes.
* Serialization of data to pass it to another process. We use `cloudpickle`
  here, which is Python only. Multiprocessing with Dragon uses standard pickle,
  as default, which doesn't support lambdas.


.. code-block:: python
    :linenos:
    :caption: **queue_demo.py: Parallel Producer - Consumer Communication with Dragon & Multiprocessing Queue**


    import random
    import string
    import cloudpickle

    import dragon
    import multiprocessing as mp


    def producer(serialized_args: bytes) -> None:
        """Generate some string data, bundle it up with some random functions, add
        it to a queue.

        :param pickled_args: arguments to the function
        :type funcs: bytes
        """

        q, funcs = cloudpickle.loads(serialized_args)

        data = random.choices(string.ascii_lowercase, k=1)[0]
        for i in range(5):
            data = data + " " + "".join(random.choices(string.ascii_lowercase, k=i + 3))

        print(
            f'I am producer {mp.current_process().pid} and I\'m sending data: "{data}" and string ops:', end=" "
        )

        n = random.randint(1, len(funcs))
        chosen = random.sample(list(funcs.items()), n)  # random selection without replacement

        for item in chosen:
            print(item[0], end=" ")
        print(flush=True)

        work_pkg = cloudpickle.dumps((chosen, data))

        q.put(work_pkg)


    def consumer(q: mp.queues.Queue) -> None:
        """Retrieve data from a queue, do some work on it and print the result.

        :param q: Queue to retrieve from
        :type q: mp.queues.Queue
        """

        # gives multi-node compatible Dragon puid, not OS pid.
        print(f"I am consumer {mp.current_process().pid} --", end=" ")

        serialized_data = q.get()  # implicit timeout=None here, blocking
        funcs, data = cloudpickle.loads(serialized_data)

        for identifier, f in funcs:
            print(f'{identifier}: "{f(data)}"', end=" ")
            data = f(data)
        print(flush=True)


    if __name__ == "__main__":

        mp.set_start_method("dragon")

        # define some string transformations
        funcs = {
            "upper": lambda a: a.upper(),
            "lower": lambda a: a.lower(),
            "strip": lambda a: a.strip(),
            "capitalize": lambda a: a.capitalize(),
            'replace(" ", "")': lambda a: a.replace(" ", ""),
        }

        # use a queue for communication
        q = mp.Queue()

        # serialize producer arguments: Dragon uses pickle as default that doesn't
        # work with lambdas
        serialized_args = cloudpickle.dumps((q, funcs))

        # create & start processes
        processes = []
        for _ in range(8):
            p = mp.Process(target=producer, args=(serialized_args,))
            processes.append(p)
            p = mp.Process(target=consumer, args=(q,))
            processes.append(p)

        for p in processes:
            p.start()

        # wait for processes to finish
        for p in processes:
            p.join()


when run with `dragon queue_demo.py`, results in output similar to the following:

.. code-block:: console
    :linenos:

    >$dragon queue_demo.py 
    I am producer 4294967297 and I'm sending data: "n jqc vneb itfqd eygjfc ljwzrfa" and string ops: capitalize upper lower strip 
    I am consumer 4294967298 -- capitalize: "N jqc vneb itfqd eygjfc ljwzrfa" upper: "N JQC VNEB ITFQD EYGJFC LJWZRFA" lower: "n jqc vneb itfqd eygjfc ljwzrfa" strip: "n jqc vneb itfqd eygjfc ljwzrfa" 
    I am producer 4294967301 and I'm sending data: "l xpp fvjh odgqi cmhxqa syxgnvl" and string ops: lower 
    I am consumer 4294967300 -- lower: "l xpp fvjh odgqi cmhxqa syxgnvl" 
    I am producer 4294967299 and I'm sending data: "w ebz uwjc ahpxw cmpfac uxyuoyd" and string ops: capitalize strip lower replace(" ", "") upper 
    I am consumer 4294967302 -- capitalize: "W ebz uwjc ahpxw cmpfac uxyuoyd" strip: "W ebz uwjc ahpxw cmpfac uxyuoyd" lower: "w ebz uwjc ahpxw cmpfac uxyuoyd" replace(" ", ""): "webzuwjcahpxwcmpfacuxyuoyd" upper: "WEBZUWJCAHPXWCMPFACUXYUOYD" 
    I am producer 4294967303 and I'm sending data: "x yga ysbv jqbvu eoryiv wemvydd" and string ops: upper lower replace(" ", "") capitalize strip 
    I am consumer 4294967306 -- upper: "X YGA YSBV JQBVU EORYIV WEMVYDD" lower: "x yga ysbv jqbvu eoryiv wemvydd" replace(" ", ""): "xygaysbvjqbvueoryivwemvydd" capitalize: "Xygaysbvjqbvueoryivwemvydd" strip: "Xygaysbvjqbvueoryivwemvydd" 
    I am producer 4294967305 and I'm sending data: "m evl kaaq bbamw yuxces mflukgc" and string ops: replace(" ", "") 
    I am consumer 4294967304 -- replace(" ", ""): "mevlkaaqbbamwyuxcesmflukgc" 
    I am producer 4294967311 and I'm sending data: "r zdv gqni phjop rxxnjv mwnoavn" and string ops: lower upper replace(" ", "") capitalize 
    I am consumer 4294967308 -- lower: "r zdv gqni phjop rxxnjv mwnoavn" upper: "R ZDV GQNI PHJOP RXXNJV MWNOAVN" replace(" ", ""): "RZDVGQNIPHJOPRXXNJVMWNOAVN" capitalize: "Rzdvgqniphjoprxxnjvmwnoavn" 
    I am producer 4294967307 and I'm sending data: "j njm pnpg spkvg bfsukk ihfmklm" and string ops: capitalize strip lower upper replace(" ", "") 
    I am consumer 4294967310 -- capitalize: "J njm pnpg spkvg bfsukk ihfmklm" strip: "J njm pnpg spkvg bfsukk ihfmklm" lower: "j njm pnpg spkvg bfsukk ihfmklm" upper: "J NJM PNPG SPKVG BFSUKK IHFMKLM" replace(" ", ""): "JNJMPNPGSPKVGBFSUKKIHFMKLM" 
    I am producer 4294967309 and I'm sending data: "a eij rzuz rlilc jkiaxr raqzvft" and string ops: replace(" ", "") capitalize upper strip lower 
    I am consumer 4294967312 -- replace(" ", ""): "aeijrzuzrlilcjkiaxrraqzvft" capitalize: "Aeijrzuzrlilcjkiaxrraqzvft" upper: "AEIJRZUZRLILCJKIAXRRAQZVFT" strip: "AEIJRZUZRLILCJKIAXRRAQZVFT" lower: "aeijrzuzrlilcjkiaxrraqzvft" 
    +++ head proc exited, code 0

Note that producers and consumers are using their Dragon `puid` instead of their
OS `pid` to identify themselves. On a distributed system, the `pid` is not
unique anymore. Dragon generalizes the concept into unique IDs that identify
managed objects across even federated systems.