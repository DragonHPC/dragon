import dragon
import multiprocessing as mp

class PrimeNumberPipeline:
    SENTINEL = 0
    MAX_QUEUE_DEPTH = 10

    # The stage_size specifies how many prime numbers to keep in each stage of the pipeline.
    # The max_number is the largest number to count to.
    def __init__(self, stage_size=100, max_number=99999999):
        if stage_size <= 0:
            raise ValueError('Cannot create PrimeNumberPipeline with stage_size of 0 or less.')

        if max_number < 2:
            raise ValueError('Must provide a maximum number greater than 1 for a PrimeNumberPipeline')

        self._stage_size = stage_size
        self._max_number = max_number

    @staticmethod
    def source(limit, pipeline_queue):
        # This serves as the source for all the primes.
        # The values are written to the pipeline_queue to
        # serve as a source for the pipeline. We start at 3
        # because we prime the first pipeline stage with 2.
        for i in range(3, limit+1):
            pipeline_queue.put(i)

        # Used to signal the end of the pipeline.
        pipeline_queue.put(PrimeNumberPipeline.SENTINEL)

    @staticmethod
    def is_relatively_prime(number, primes):
        # Check that number is relatively prime to the primes.
        for prime in primes:
            # This is an optimization
            if number < prime*2:
                return True

            if number % prime == 0:
                return False

        return True

    @staticmethod
    def pipeline_stage(stage_size, prime, in_queue, out_queue):
        # A stage is given a prime from which to start. All other
        # primes it finds (up to the stage size) are added to this
        # list of primes to check.
        primes = [prime]
        stage_proc = None

        # This is the number of primes (relative to primes) that this
        # stage has found.
        cur_count = 1

        while True:
            number = in_queue.get()

            # We terminate via a sentinel value.
            if number == PrimeNumberPipeline.SENTINEL:
                out_queue.put(PrimeNumberPipeline.SENTINEL)
                if stage_proc is not None:
                    stage_proc.join()
                print(f'\nprimes in pipeline stage are {primes}', flush=True)
                return

            if PrimeNumberPipeline.is_relatively_prime(number, primes):
                # It is relatively prime, so send it to the output queue
                out_queue.put(number)

                # If it is found to be relatively prime, we add it to the
                # list of primes or we create a new stage with it (which
                # in turn builds its list of primes).
                if cur_count < stage_size:
                    primes.append(number)

                elif cur_count == stage_size:
                    # create a new pipeline stage
                    new_stage_queue = mp.Queue(maxsize=PrimeNumberPipeline.MAX_QUEUE_DEPTH)
                    stage_proc = mp.Process(target=PrimeNumberPipeline.pipeline_stage, args=(stage_size, number, new_stage_queue, out_queue))
                    out_queue = new_stage_queue
                    stage_proc.start()
                else:
                    # It was checked/will be checked by other stages in the pipeline
                    pass

                # Number of relatively prime primes found for this stage.
                cur_count+=1



    def start(self):
        self._source_queue = mp.Queue(maxsize=PrimeNumberPipeline.MAX_QUEUE_DEPTH)
        self._sink_queue = mp.Queue(maxsize=PrimeNumberPipeline.MAX_QUEUE_DEPTH)
        self._source_proc = mp.Process(target=PrimeNumberPipeline.source, args=(self._max_number, self._source_queue))
        self._source_proc.start()
        self._sink_queue.put(2) # "Prime" the sink queue ;)
        self._stage_proc = mp.Process(target=PrimeNumberPipeline.pipeline_stage, args=(self._stage_size, 2, self._source_queue, self._sink_queue))
        self._stage_proc.start()

    def stop(self):
        self._source_proc.join()
        self._stage_proc.join()

    def get(self):
        return self._sink_queue.get()


def main():
    # We set the start method to dragon to use dragon multiprocessing.
    mp.set_start_method('dragon')

    # We can control the pipeline by max_number and stage_size. The
    # stage_size can control the amount of parallelism.
    prime_pipeline = PrimeNumberPipeline(stage_size=3, max_number=100)
    prime_pipeline.start()
    print('Prime numbers:')
    count = 0
    for prime in iter(prime_pipeline.get, PrimeNumberPipeline.SENTINEL):
        text = f'{prime} '
        print(text, end="", flush=True)
        count+= len(text)
        if count > 80:
            count = 0
            print(flush=True)
    print(flush=True)
    prime_pipeline.stop()


if __name__ == "__main__":
    main()