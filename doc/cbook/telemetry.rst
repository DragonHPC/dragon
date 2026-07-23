Telemetry
+++++++++

Dragon's telemetry stack lets you collect runtime and application metrics, move
them through the Dragon services layer, and visualize them in external tools
such as Grafana. Start here if you want to understand what telemetry support is
available before diving into a full example.

The example below shows the basic telemetry path in practice: how Dragon emits
metrics, how those metrics can be observed while a workload is running, and how
to use that data when debugging or tuning an application.

.. toctree::
    :maxdepth: 1

    dragon_telemetry.rst