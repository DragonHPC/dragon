.. _Overlay_Network:

Dragon Overlay Network
++++++++++++++++++++++

In the multi-node case, Dragon establishes an Overlay Network to communicate between the Dragon Launcher FrontEnd and
the Dragon Launcher Backend processes running on each backend compute node. To establish this Overlay Network, Dragon uses 
the Dragon TCP Network Agent in a Fanout Tree. The Fanout Tree uses a default branching factor of 32 nodes. The hierarchy
of the tree follows the order of nodes in the node list with the root being the front end and children being located at 
bn+1 to bn+b where b is the branching factor (32) of the tree. This enables Dragon to scale and communicate with a large
number of nodes efficiently.

:numref:`overlay-network-fanout` shows an example of a 10,000 node Dragon Overlay Network Fanout. 

.. figure:: images/overlay_network_fanout.svg
    :name: overlay-network-fanout 

    **Example 10,000 node Overlay Network fanout**