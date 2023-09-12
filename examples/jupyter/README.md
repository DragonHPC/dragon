# Jupyter Running Inside Dragon

This example shows how to run a Jupyter notebook inside Dragon. It
is a first example only. More integration with Dragon will be needed
for a tighter experience, but this example shows how this might be done.

To run the example, from the terminal window run

    dragon start_jupyter.py

This will print a URL for the loopback interface to open in a web browser.
Copy and paste that 127.0.0.1 URL to your favorite browser. Then open
the JupyterWithDragon notebook and run the cells in it. You'll see

    Hello Dragon!!

printed to the screen if all worked correctly, which was printed from a
multiprocessing Process, potentially running on a different node should
this be run in a multinode configuration with no additional changes
necessary for this demo to work multinode.

The example.py code contains scaffolding code to make this demo work.

You can read more about this in the Dragon documentation under the
Cookbook. There is a section in the Cookbook on running Jupyter
Notebooks either singlenode or multinode.

In this directory there is also an example of capturing output
from a process. The getout.py demonstrates how this can be done
using a *start capturing* command and a *stop capturing* command.


# Distributed inference with a Large Language Model (LLM) and node telemetry

This example presents an application where we perform distributed inference by
using an LLM with the Dragon runtime and standard Python multiprocessing
interfaces. We also perform node monitoring and visualization. Specifically, we
run a chatbot service where a user can interact with by providing questions and
getting back responses.  We use a Jupyter notebook with Dragon as the front end
where the user can provide prompts/queries.  For the telemetry component, we use
Prometheus Server to generate time-series data which are then ported for
visualization into Grafana.  We used the Blenderbot
(https://huggingface.co/docs/transformers/model_doc/blenderbot) chatbot as our
language model to respond to the prompts input by the user.

Our process architecture is as follows.  We create a pool of workers, referred
to as inference workers, that perform inference. We also create another pool of
workers that are responsible for gathering telemetry data, and we
start one process on each node in our allocation (`telemetry workers`). Last, we
have a third pool of workers, the `response workers`, which are responsible for
returning the correct answer to the correct prompt. We also use two Dragon
queues that are shared among the processes and nodes.  The first one (`prompt
queue`) is used for the inference work items from which the inference workers get
work. The second one (`response queue`) is used for the responses; each
inference worker puts the response into this queue and the response workers get
each response and correspond it to the correct prompt.

In this example, we place four inference workers across two nodes.  Each worker
utilizes a single Nvidia A100 GPU to perform the inference on the prompt. When a
prompt is input by the user, the prompt and a prompt ID are placed into the
`prompt queue` that is shared among all the inference workers. The inference
workers greedily grab from this queue, generate a response, and place the
response and a response ID with the original prompt and the prompt ID into the
`response queue`.  We then simulate an influx of prompts and, using the
telemetry data, visualize the ability to balance this load among the inference
workers.

The implementation of an inference worker is in `llm_backend.py` file.  The
queue that the response is placed in, `q_out`, is shared among two response
workers that parse the response and return the prompt, prompt ID, response, and
response ID back to the prompter. In this case, that is done by printing this
output; however, if you have multiple users, the response workers would be
responsible for returning the response to the correct prompt ID. The structure
of a `response worker` is similar to that of an `inference worker` in that each
worker enters a while loop where they greedily get from the shared
`response queue` and exit when the queue is empty and the end event is set.

Each of the `telemetry workers` executes the `telem_work()` function, inside
`telemetry.py` file. This function includes the metrics for the telemetry data
in Prometheus-compatible format.  We define seven metrics in total
(`gpu_utilization`, `gpu_memory_utilization`, `gpu_memory_used`,
`gpu_memory_free`, `gpu_memory_total`, `system_load_average`,
`request_latency`), which we update every second until the end event is set. We
also start Prometheus metrics server and we set the port to `8000`.

Similar to the Multi-node process orchestration and node telemetry example,
which can be found in the Solution Cookbook, one `telemetry worker` is placed
on each node and collects gpu utilization data as well as the cpu load average
over one minute for that node.  Unlike that example, this data is pushed to a
Prometheus server that generates time-series data that can be visuzlized using
Grafana.  This allows the user or administrator to monitor the load across the
inference workers. We use it to visualize how the load is balanced when a large
number of users all input prompts. In this case we loop over a list of fifteen
prompts seven times giving a total of 105 prompts for the inference workers to
respond to.

More information about this example can be found in the Dragon documentation's
Solution Cookbook.

### Installation

After installing dragon, the remaining packages needed to install are located in
the `requirements_llm.txt` file.  The version of PyTorch and its dependencies
may need to be changed to run on other systems.

```
> pip install -r requirements_llm.txt
```

Alternatively, the packages and their dependencies can be installed
individually. The PyTorch version and corresponding pip command can be found
here: https://pytorch.org/get-started/locally/.

```
> pip install torch torchvision torchaudio
> pip install py3nvml
> pip install huggingface-hub
> pip install transformers
```


#### Prometheus Server

You can find information on how to install and configure Prometheus server in
the Getting started Prometheus page:
https://prometheus.io/docs/prometheus/latest/getting_started/.

In our case, we used a system named `pinoak` to run the server. Note that it can
run on the login node and there is no need to use a compute node for the server.

Assuming that you have successfully installed the server, next you need to
update prometheus yaml file. One of the main fields is the targets that the
server will scrape data from, i.e. in our case, the compute node(s)' hostnames
that we used to run our application that generates the telemetry metrics.  In
our example, we used the same system to run the Prometheus server and our
application (`pinoak`). We requested an allocation of two nodes to run our
inference application (`pinoak0043` and `pinoak0044`).

Here follows a sample yaml file. The server scrapes data from two nodes in our
example, `pinoak0043` and `pinoak0044`, which we provide as the scrape targets
along with the port.

```
# my global config
global:
scrape_interval: 5s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
evaluation_interval: 5s # Evaluate rules every 15 seconds. The default is every 1 minute.
# scrape_timeout is set to the global default (10s).

# Alertmanager configuration
alerting:
alertmanagers:
    - static_configs:
        - targets:
        # - alertmanager:9093

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
#scrape_configs:
# The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
# - job_name: "prometheus"

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

#static_configs:
#   - targets: ["localhost:9090"]

scrape_configs:
- job_name: 'telemetry_full'
    static_configs:
    - targets: ['pinoak0043:8000', 'pinoak0044:8000']
```

The above yaml file is also provided as `example_prometheus.yml` in this
directory.  Just make sure to rename it to `prometheus.yml` if you plan to use
it as your prometheus configuration file, otherwise you'll need to provide
`--config.file` argument with your configuration file name in the run command.
Remember that in our application, we set the port for the metrics port to
`8000`.

Last, we start the server with the following command:

```
cd prometheus_folder
./prometheus
```

#### Grafana Server

First, we need to install Grafana in a system. We follow instructions from the
Grafana official documentation
https://grafana.com/docs/grafana/latest/setup-grafana/installation/.

Assuming that we have it installed, we then start the Grafana server with the
following command:

```
cd grafana_folder
./bin/grafana-server web
```

Then, on our local computer we set up a tunnel as follows:

```
ssh -NL localhost:1234:localhost:3000 username@system_name
```

where `system_name` is the system where we installed and run Grafana.

Finally, we access Grafana in our web browser via the following URL:

```
http://localhost:1234
```

To complete the setup and have Prometheus server communicate and send data to
Grafana, we need to configure Grafana via the web browser interface. We need to
create a new Prometheus data source by following the instructions here:
https://grafana.com/docs/grafana/latest/datasources/prometheus/. The most
important field is the `URL`, where we need to provide the URL (ip address and
port) of the system that Prometheus server runs on. For example, in our case it
was `http://pinoak.us.cray.com:9090`.  Last, we need to create a new dashboard
for visualizing our metrics. You can find information here:
https://grafana.com/docs/grafana/latest/dashboards/.

### Usage

To run this example, follow the multi-node start up instructions for running a
Jupyter notebook inside of Dragon (see the Solution Cookbook) and then open the
`llm_example.ipynb` notebook which can be found in this directory. In order for
the telemetry component to work and visualize the data with Grafana,
you will need to also have the Prometheus and Grafana servers started by
following the instructions above.
.