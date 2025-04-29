.. _DragonCoreFLICython:

File Like Interface
+++++++++++++++++++++++++

This is the FLI API for Python. The classes presented here are a thin wrapper of the C API. The C language description of the :ref:`DragonFileLikeInterface`
provides a detailed description of the FLI code and should be consulted for a good overview of this functionality. This section provides the
description of the Python interface to this C code.

.. contents::
    :depth: 3
    :local:
    :backlinks: entry

Classes
=======

.. automodule:: dragon.fli
    :members: FLInterface, FLISendH, FLIRecvH, DragonFLIError, FLIEOT

Exceptions
==========

.. automodule:: dragon.fli
    :members: DragonFLIError, FLIEOT


