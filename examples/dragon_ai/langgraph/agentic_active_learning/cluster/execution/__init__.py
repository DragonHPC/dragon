"""What Dragon does for the graph.

`launch` creates and destroys the stores, the Batch runtime, and the optional
on-cluster LLM. `clients` is how a node reaches them: attached DDicts, Batch
task polling, and placement. `worker` is the one owner of every running job,
reader, and fit on the worker host. `steering` inspects running jobs and asks
Dragon to stop the bad ones while they are still computing.

Nothing here is science. Nothing here is a graph node either; campaign.py
calls in, and these modules call Dragon.
"""
