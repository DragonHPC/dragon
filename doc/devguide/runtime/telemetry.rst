.. _Telemetry:

Telemetry
+++++++++++++++++++++++++++++++++

The Telemetry infrastructure in the Dragon runtime is a way for users to visualize real-time metrics while running their application.

Adding the telemetry flag launches an additional head process that runs alongside the application. The telemetry application shares dragon infrastructure 
like global services, local services, transport agents, and the launcher frontend and backend. In Figure :numref:`telemetry-architecture` the architecture of 
the telemetry service is laid out. The user is responsible for installing grafana locally and setting up the ssh port forwarding from the aggregator node, 
which will be one of the compute nodes, through the login node, to their laptop. Future versions of dragon will set up some of these for the user.


.. figure:: ../images/architecture_telemetry.png
  :scale: 10%
  :name: telemetry-architecture

**Telemetry Architecture**

Code structure
==============

Telemetry code is primarily in ``src/dragon/telemetry/``

==================  ==============
file                contents
------------------  --------------
aggregator_app.py   Aggregator Flask API
collector.py        Collector object to retrieve default metrics
dragon_server.py    Interface to process queries
telemetry_head.py   Head process for startup sequence, and Aggregator Gunicorn Server
telemetry.py        Telemetry API for user application
tsdb_app.py         TSDB Flask App to insert and update local database
tsdb_server.py      TSDB Gunicorn Server
==================  ==============

.. _TelemetryArchitecture:

Telemetry Architecture
=======================

Telemetry has a few settings that can be customized using a YAML file. To do this, provide the path to the file by setting the ``DRAGON_TELEMETRY_CONFIG`` 
environment variable. Keys that can be set in the YAML are included in the relevant sections. A sample YAML is available in ``/examples/dragon_telemetry/telemetry.yaml.

Local Database
-----------------

An SQLite3 database located on each compute node. By default the location of this database is the ``/tmp`` directory. This can
be changed by specifying the desired directory by setting the ``default_tmdb_dir`` key in the YAML.
This database is created during startup (if it does not exist). Naming convention for the database is - ``ts_<hostname>.db``.
It contains two tables with the following schema -

Table: metrics
```````````````

===============     ==============  ============
column_name         type            description
---------------     --------------  ------------
metric              text            metric name
timestamp           text            timestamp
value               real            metric value
tags                BLOB            tag key and tag value
===============     ==============  ============

Sample dps

``{
    "dps": [
        {
            "metric": "load_average",
            "value": 0.3115234375
        },
        {
            "metric": "used_RAM",
            "value": 6.2
        },
        {
            "metric": "DevicePowerUsage",
            "value": 89.0,
            "tags": {
                "gpu": 0
            }
        },
        {
            "metric": "DeviceUtilization",
            "value": 0,
            "tags": {
                "gpu": 0
            }
        }
    ],
    "timestamp": 1746652796
}``

Table: flags
```````````````

===============     ==============  ============
column_name         type            description
---------------     --------------  ------------
is_shutdown         BLOB            multiprocessing.Event
===============     ==============  ============

Server
------

This is a Flask app served with Gunicorn. It handles inserts and updates to the local database.
The following APIs are exposed by the application -

===============================  =======  ============
endpoint                         method   description
-------------------------------  -------  ------------
``/api/metrics``                 POST     Insert metrics into metrics table
``/api/set_shutdown``            GET      Set shutdown Event
``/api/set_telemetry_shutdown``  GET      Begin shutdown sequence
``/api/tsdb_cleanup``            POST     Remove timeseries data before a specific timestamp
===============================  =======  ============


Collector
---------

Collects default metrics and uses Server API to insert them into the local database. Using the key ``default_tmdb_window`` in the YAML, the Collector checks if
the current time window of collected metrics exceeds the user set window. If it does, it sends a request to the Server to clean up datapoints that fall before the window.

Note: The value of ``default_tmdb_window`` is in seconds. By default, the value is 300 seconds.


Aggregator
----------

A Flask app served with Gunicorn. Implements APIs that Grafana interacts with.

===============================  =======  ============
endpoint                         method   description
-------------------------------  -------  ------------
``/api/aggregators``             GET      Returns a static list of aggregator functions
``/api/query``                   POST     Sends query from Grafana to node queues and returns response from nodes
``/api/suggest``                 GET      Auto-suggests metric names using a given query string
``/api/set_telemetry_shutdown``  GET      Signals shutdown sequence to Aggregator (and Gunicorn server)
===============================  =======  ============

Dragon Server
-------------

This is an interface to retrieve time series data from the local database based on the query sent by the Aggregator.
It continuously listens to the request queue, retrieves query requests, and puts them to a return queue after processing them.

Grafana
--------
Dashboard to view time series data. We have created a customized dashboard that has been exported as a JSON config.
This config can be imported to your Grafana instance.

Queues
-------
Each compute node has a Request Queue associated with it. This is the queue the Aggregator forwards requests to.
There is one Return Queue where all Dragon Servers return query responses.
The Aggregator retrieves these responses from the queue.


YAML Configuration options
===========================

* ``aggregator_port``

  * Port where Aggregator should receive requests from Grafana.
  * Default: 4242

* ``tsdb_server_port``

  * Port where TSDB Server starts on (same for every compute node).
  * Default: 4243

* ``collector_rate``

  * Collector metric collections frequency.
  * Default: 0.5 seconds

* ``default_tmdb_window``

  * Default time window for time series database cleanup. 
  * Default: 300 seconds

* ``default_tmdb_dir``

  * Default directory to store local tsdb database.
  * Default: /tmp

* ``delete_tmdb``

  * Specify whether local tsdb database should be deleted post cleanup. Setting it to 1 will delete it.

* ``remote_port_forward``

  * Specify the login node. If set, the Aggregator server will execute an ssh tunnel from compute node to login node.

Telemetery Service Startup
===========================

When the user specifies the telemetry flag while running dragon, a separate telemetry head process is started along with the user application.

``telemetry_head.py``
---------------------
Orchestrates start up.
1. Creates a set of required objects -
   - A dictionary of queues associated with each compute node (Request queues)
   - A Return queue
   - A shutdown Event
2. Starts Dragon Server process on each compute node
3. Starts the Aggregator process
   - Receives request from Grafana on port ``4242``
   - Sends requests to each Request queue

``dragon_server.py``
--------------------
Manages Collector, TSDB server process, and listens to Request queue for queries

1. Starts TSDB Server process
2. Starts Collector process
3. Starts Dragon Server listener process
   - Listener process checks Request Queue for incoming query messages
   - Creates a response and puts it to Return queue
   - Checks if shutdown Event has been set in local database
   - If yes, initiates shutdown processs across TSDB Server, and Aggregator (if running on the same node)

``collector.py``
----------------

1. Collects metrics and constructs them into a specific structure
2. Sends structure to TSDB Server if start Event is set
3. If current data size exceeds ``default_tmdb_window``, sends a cleanup request to TSDB Server
4. Exits if shutdown Event has been detected

``tsdb_server.py``
-------------------
A custom gunicorn application

1. Loads custom config and binds to port defined in ``tsdb_server_port`` or ``4243`` (default)
2. Loads TSDB App
   - Inserts and updates metrics
   - Cleans up metrics
3. Sets start Event when ready (for collector)
4. Exits if shutdown request is detected


To Do
=======
Phase 1
--------
1. Scalability
2. Divide metrics along different telemetry levels, provide Grafana configs for each level (in progress)
3. Standardize messaging between Aggregator and Dragon Servers using routing decorator.
4. Add timeout for Telemetry initialization. Currently, when the user calls Telemetry(), it blocks until the TSDB Server is started by Telemetry.
5. Allow users to specify size limit of DB
6. Allow users to adjust frequency of default metric collection

Phase 2
--------
1. Way for users to dump DBs to persistent storage for post-run processing.
2. User API - Add Filter class that allows users to define functions that filter data both at collection time and when a request from Grafana is made.


Phase 3
----------
1. Revamp Dragon Server - use Ddict, messages (local node discovery)
2. Simplify Aggregator App /api/suggest (push to pull)
 
Someday
----------
1. Dragon Plugin to replace OpenTSDB Plugin