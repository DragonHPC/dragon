Running Jupyter Notebook inside of the Dragon
++++++++++++++++++++++++++++++++++++++++++++++

It is possible to run a Jupyter notebook inside of Dragon. This gives the user of
the notebook full access to the distributed multi-node implementation of
multiprocessing that Dragon provides.

There are two modes of Dragon, the single node version and the multi node
version. The single node version, running locally on a laptop or desktop computer
requires a little less setup. The multi node version, depending on your network
configuration, requires some port forwarding to get the jupyter kernel server
port to an externally visible location.

Single Node Jupyter on a Desktop or Laptop
----------------------------------------------

To start
Jupyter locally, you need to make sure you have a correct installation of
Dragon which has installed all the Python requirements. For instance, the
jupyter package must be pip installed if it is not available. You can check
this by starting up the Python REPL (Read/Eval/Print Loop) and trying *import
jupyter*. If that succeeds, you have the jupyter package installed. Once you
have the prerequisites for Dragon and Jupyter, you can proceed as follows.

Within the *examples* directory is a subdirectory named *jupyter*. Go to that
directory and execute the *dragon-jupyter* program as indicated here.

.. code-block:: text
    :linenos:

    root ➜ /workspaces/hpc-pe-dragon-dragon $ cd examples/jupyter/
    root ➜ /workspaces/hpc-pe-dragon-dragon/examples/jupyter $ dragon-jupyter
    [I 19:10:33.781 NotebookApp] Writing notebook server cookie secret to /root/.local/share/jupyter/runtime/notebook_cookie_secret
    [I 19:10:34.424 NotebookApp] Serving notebooks from local directory: /workspaces/hpc-pe-dragon-dragon/examples/jupyter
    [I 19:10:34.424 NotebookApp] Jupyter Notebook 6.5.2 is running at:
    [I 19:10:34.424 NotebookApp] http://0234599f72d3:8888/?token=961a1ff02c724743eccf2b24fbfc89559b77c9ad1f421d60
    [I 19:10:34.424 NotebookApp]  or http://127.0.0.1:8888/?token=961a1ff02c724743eccf2b24fbfc89559b77c9ad1f421d60
    [I 19:10:34.424 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
    [C 19:10:34.429 NotebookApp]

        To access the notebook, open this file in a browser:
            file:///root/.local/share/jupyter/runtime/nbserver-10193-open.html
        Or copy and paste one of these URLs:
            http://0234599f72d3:8888/?token=961a1ff02c724743eccf2b24fbfc89559b77c9ad1f421d60
        or http://127.0.0.1:8888/?token=961a1ff02c724743eccf2b24fbfc89559b77c9ad1f421d60

You will see output like what is shown here. The last line, the 127 local address can be pasted into a webbrowser to
start up Jupyter on your local machine running inside the Dragon run-time. If you open the JupyterDragon notebook
that you see in the examples directory, you can execute the cells in it to give you a brief introduction to using
Dragon's implementation of Python multiprocessing.

When you are done, a Ctl-C in the terminal where you started the jupyter server will bring it all down and exit
the Dragon run-time services.

Multi-Node Jupyter
---------------------

To run a Jupyter Notebook inside of Dragon in a multinode setup, you must first
get an allocation on your system of choice. Once you have an allocation, you can
then run, with dragon, the same command as in the single node case. When you do so,
you'll see output as shown above. Then you have to set up a tunnel to your laptop
to run the Jupyter notebook. It is likely that the tunnel has to go through the login
node of the machine. So, on the login node you start a tunnel.

.. code-block:: text
    :linenos:

    ssh -NL localhost:1234:localhost:8888 <user@node>

Then, on your laptop you start another tunnel.

.. code-block:: text
    :linenos:

    ssh -NL localhost:1234:localhost:1234 <user@system_name>

This will allow you to connect on your laptop to port 1234. If that port were taken, you can use
whatever port you like. The first time you connect you'll be asked for a token. The token
it refers to is the token in the URL that was printed to the login node when the dragon-jupyter
was started.

That's it. You now have the ability to create Jupyter Notebooks with full access to all the
nodes in your supercomputer allocation.

Jupyter Examples
----------------

.. toctree::
    :maxdepth: 2

    ../cbook/basic_pandarallel_demo.rst
    ../cbook/bioinfo_alignment_pandarallel_demo.rst
