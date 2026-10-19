************
Generalities
************


Fork vs spawn
=============

``multiprocessing`` supports both 'fork' and 'spawn' methods for starting new interpreters
under Unix.  The 'fork' method is **considered harmful** because it violates the
principle of every object knowing who its owner really is and every interpreter
knowing what objects it exclusively owns.

Therefore all the tests include::

        multiprocessing.set_start_method('spawn')

