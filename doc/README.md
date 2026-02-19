Welcome to the Dragon Documentation Launchpad Repository
==========================================================

To make changes you edit the index.rst file or the appropriate rst
file linked from index.rst either directly or indirectly.

To make changes and view them locally you can type

    make
    open _build/html/index.html

To start fresh you can type

    make clean
    make

On the first build after a ``make clean'' you will
notice many warning messages. These are due to sphinx
warning about duplicate tags usually which are not really
an issue but seems to show up when making the first time.
If you execute one more ``make'' then error messages you see
are legitimate and should be addressed with any updates to the
documentation.

To release the updates to the Dragon website type

    make release

If you are not updating doxygen comments in source code
then you can type

    make html

to update the docs. A make defaults to 'make all'
and rebuilds the doxygen xml files for inclusion
in our documentation. This step can be skipped if has
already been done and no updates were made in doxygen
comments.

The website is updated automatically,
from the master branch,
so executing *make release* should be
unnecessary unless you want updated releases sooner.

This repository is under GIT control. Source changes
should be committed to the repository.


Editing the Documentation
-------------------------

* You can reference items by CamelCasing their names like this: :ref:`GlobalServices`. If you're adding components, please add an appropriate link.
* The content of a file should describe the architecture diagram it shows, i.e. the subcomponents component and exported APIs.
* The headline hierarchy is + then = then -
* We are using 4 space indents
* If something is missing use a **FIXME** to notify the reader
* No linebreaks are used. Your editor can wrap it for you and rst doesnt have good support for it anyway.

Dependencies
------------

* SRMStofig: http://github.com/kentdlee/SRMStoFigs
* Java to run plantuml
* Doxygen
