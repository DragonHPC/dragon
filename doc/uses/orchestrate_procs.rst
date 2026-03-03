.. _orchestrate_procs:

.. currentmodule:: dragon.mpbridge.context

Orchestrate Processes
+++++++++++++++++++++

Dragon provides its own native API to finely and programmatically control where and how processes get
executed. Below, we work thorough the use of :ref:`dragon.native <NativeAPI>` to execute a combination of
user applications, specify their placement on hardware, and how to manage their output

ProcessGroup
============

Anytime you have some number of processes you want to execute, :py:class:`~dragon.native.process_group.ProcessGroup` is
where you want to begin. In fact, :py:class:`~dragon.native.process_group.ProcessGroup` is so powerful that Dragon uses
it as the backbone for its implementation of :py:meth:`~dragon.mpbridge.context.DragonContext.Pool`.


Hello World!
------------

We'll begin by just executing the classic "Hello world!" example. In the snippet, we begin
by creating a :py:class:`~dragon.native.process_group.ProcessGroup` object that contains all the API for managing the
processes we'll assign to it. We'll assign processes to the group by defining a
:py:class:`~dragon.native.process.ProcessTemplate`.

A :py:class:`~dragon.native.process_group.ProcessGroup` can contain
as many templates as we'd like, and we can also tell :py:class:`~dragon.native.process_group.ProcessGroup` how many
instances of a given template we want to execute. In this example, we'll launch 4 instances of
our `"Hello World!"` template. After all that setup is complete, we'll initialize the infrastructure for the the
:py:class:`~dragon.native.process_group.ProcessGroup` object and start execution of the 4 `"Hello World!"` instances.
We then tell our :py:class:`~dragon.native.process_group.ProcessGroup` object to join on the completion of those 4
instances and then close all the :py:class:`~dragon.native.process_group.ProcessGroup` infrastructure

.. code-block:: python
    :linenos:
    :caption: **Execute a group of "Hello world!" processes with ProcessGroup**

    import socket
    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate


    def hello_world():
        print(f'hello from process {socket.gethostname()}!')


    def run_hello_world_group():

        pg = ProcessGroup()
        hello_world_template = ProcessTemplate(target=hello_world)
        pg.add_process(nproc=4, template=hello_world_template)

        pg.init()
        pg.start()

        pg.join()
        pg.close()


    if __name__ == '__main__':

        run_hello_world_group()


Defining Multiple Templates
---------------------------

Say you'd like to run different applications but have them be part of the
same :py:class:`~dragon.native.process_group.ProcessGroup`. That is easily done by providing multiple templates
to your :py:class:`~dragon.native.process_group.ProcessGroup` object. In the following example, we'll create a data
generator app and a consumer of that data that will be connected to each other via a
:py:class:`~dragon.native.queue.Queue`. The :py:class:`~dragon.native.queue.Queue` will passed as input to each of
the processes.

.. _consumer_generator_example:

.. code-block:: python
    :linenos:
    :caption: **Run ProcessGroup with a process generating data passed to consumer via a Queue**

    import random

    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate
    from dragon.native.queue import Queue


    def data_generator(q_out, n_outputs):

        for _ in range(n_outputs):
            output_data = int(100 * random.random())
            print(f'generator feeding {output_data} to consumer', flush=True)
            q_out.put(output_data)


    def data_consumer(q_in, n_inputs):

        for _ in range(n_inputs):
            input_data = q_in.get()
            result = input_data * 2
            print(f'consumer computed result {result} from input {input_data}', flush=True)


    def run_group():

        q = Queue()
        pg = ProcessGroup()

        generator_template = ProcessTemplate(target=data_generator,
                                             args=(q, 5))
        consumer_template = ProcessTemplate(target=data_consumer,
                                            args=(q, 5))

        pg.add_process(nproc=1, template=generator_template)
        pg.add_process(nproc=1, template=consumer_template)

        pg.init()
        pg.start()

        pg.join()
        pg.close()


    if __name__ == '__main__':

        run_group()


Managing Output/stdout
----------------------

In the :ref:`above example <consumer_generator_example>`, we had a bit of redundant
output. We get the input via the generator process printed to stdout and then
that value is echoed in the consumer process:

.. code-block:: console
    :linenos:
    :caption: **Output from execution of consumer/generator example without piping generator output to `/dev/null`**

    (_env) user@hostname:~/dragon_example> dragon generator_consumer_example.py
    consumer computed result 140 from input 70
    generator feeding 70 to consumer
    consumer computed result 160 from input 80
    generator feeding 80 to consumer
    consumer computed result 14 from input 7
    generator feeding 7 to consumer
    consumer computed result 28 from input 14
    generator feeding 14 to consumer
    generator feeding 72 to consumer
    consumer computed result 144 from input 72

Since the generator information is redundant, let's send it to `/dev/null` by
modifying the driver function in :ref:`above example <consumer_generator_example>`:

.. code-block:: python
   :linenos:
   :caption: **Sending generator stdout to `/dev/null`**

    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate, Popen
    from dragon.native.queue import Queue

    def run_group():

        q = Queue()
        pg = ProcessGroup()

        # Tell the dragon to get rid of the geneator's stdout
        generator_template = ProcessTemplate(target=data_generator,
                                             args=(q, 5),
                                             stdout=Popen.DEVNULL)

        consumer_template = ProcessTemplate(target=data_consumer,
                                            args=(q, 5))

        pg.add_process(nproc=1, template=generator_template)
        pg.add_process(nproc=1, template=consumer_template)

        pg.init()
        pg.start()

        pg.join()
        pg.close()

The end result is an easier to parse stream of output:

.. code-block:: console

    (_env) user@hostname:~/dragon_example> dragon generator_consumer_sanitized_output.py
    consumer computed result 50 from input 25
    consumer computed result 30 from input 15
    consumer computed result 80 from input 40
    consumer computed result 44 from input 22
    consumer computed result 12 from input 6


Placement of ProcessGroup Processes via Policy
==============================================

Commonly, a user wants to have one process run on a particular hardware resource
(eg: GPU) while other processes are perhaps agnostic about their compute resources.
In Dragon, this is done via the :py:class:`~dragon.infrastructure.policy.Policy` API. To illustrate this, we'll take
the basic template of the consumer-generator example above and replace it with some `simple PyTorch code
<https://pytorch.org/tutorials/beginner/pytorch_with_examples.html#pytorch-tensors-and-autograd>`_
While we're not doing anything complicated or exercising this paradigm as you might in
reality (e.g., generating model data on a CPU and feeding training inputs to a GPU),
it provides a template of how you might do somethign more complicated. We'll replace the data generator function from
above with initialization of PyTorch model parameters. We'll pass these to the consumer process which will use a GPU
to train the data. And lastly, we'll use the :py:class:`~dragon.native.policy.Policy` API to specify the PyTorch model
is trained on a compute node we know has a GPU present.

.. code-block:: python
    :linenos:
    :caption: **Generating training input data on the CPU and passing to a GPU process for PyTorch training**

    from dragon.infrastructure.policy import Policy
    from dragon.native.process_group import ProcessGroup
    from dragon.native.process import ProcessTemplate
    from dragon.native.queue import Queue

    import torch
    import math


    def data_generate(q_out):

        torch.set_default_device("cpu")

        dtype = torch.float

        # Create Tensors to hold input and outputs.
        # By default, requires_grad=False, which indicates that we do not need to
        # compute gradients with respect to these Tensors during the backward pass.
        x = torch.linspace(-math.pi, math.pi, 2000, dtype=dtype)
        y = torch.sin(x)

        # Create random Tensors for weights. For a third order polynomial, we need
        # 4 weights: y = a + b x + c x^2 + d x^3
        # Setting requires_grad=True indicates that we want to compute gradients with
        # respect to these Tensors during the backward pass.
        a = torch.randn((), dtype=dtype, requires_grad=True)
        b = torch.randn((), dtype=dtype, requires_grad=True)
        c = torch.randn((), dtype=dtype, requires_grad=True)
        d = torch.randn((), dtype=dtype, requires_grad=True)

        q_out.put((x, y, a, b, c, d))


    def pytorch_train(q_in):

        torch.set_default_device("cuda")

        x, y, a, b, c, d = q_in.get()

        x.to('cuda')
        y.to('cuda')
        a.to('cuda')
        b.to('cuda')
        c.to('cuda')
        d.to('cuda')

        learning_rate = 1e-6
        for t in range(2000):
            # Forward pass: compute predicted y using operations on Tensors.
            y_pred = a + b * x + c * x ** 2 + d * x ** 3

            # Compute and print loss using operations on Tensors.
            loss = (y_pred - y).pow(2).sum()
            if t % 100 == 99:
                print(t, loss.item())

            # Use autograd to compute the backward pass.
            loss.backward()

            # Manually update weights using gradient descent.
            with torch.no_grad():
                a -= learning_rate * a.grad
                b -= learning_rate * b.grad
                c -= learning_rate * c.grad
                d -= learning_rate * d.grad

                # Manually zero the gradients after updating weights
                a.grad = None
                b.grad = None
                c.grad = None
                d.grad = None

        print(f'Result: y = {a.item()} + {b.item()} x + {c.item()} x^2 + {d.item()} x^3')


    def run_group():

        q = Queue()
        pg = ProcessGroup()

        # Since we don't care where the data gets generated, we let
        # Dragon determine the placement by leaving the placement kwarg blank
        generator_template = ProcessTemplate(target=data_generate,
                                             args=(q,))

        # node 'pinoak0033' is the hostname for a node with NVIDIA A100 GPUs.
        # We tell Dragon to use it for this process via the policy kwarg.
        train_template = ProcessTemplate(target=pytorch_train,
                                         args=(q,),
                                         policy=Policy(placement=Policy.Placement.HOST_NAME,
                                                       host_name='pinoak0033'))

        pg.add_process(nproc=1, template=generator_template)
        pg.add_process(nproc=1, template=train_template)

        pg.init()
        pg.start()

        pg.join()
        pg.close()


    if __name__ == '__main__':

        run_group()

Related Cookbook Examples
=========================

* :ref:`cbook_policy`
