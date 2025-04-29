Creating and Using a Queue in Dragon Native
-------------------------------------------

The Dragon Native Queue implementation is Dragon's specialized
implementation of a Queue, working in both single and multi node settings. 
It is interoperable in all supported languages. The API is similar to Python's
Multiprocessing.Queue in many ways, but has a number of extentions and
simplifications. In particular, the queue can be initialized as joinable, which
allows to join on the completion of an item.


Using a Queue with Python
^^^^^^^^^^^^^^^^^^^^^^^^^

TBD

Using a Queue with C
^^^^^^^^^^^^^^^^^^^^

To use a Dragon queue it's as simple as creating it and then sending data.
Managed queues incur some startup cost, but no performance cost while using and
they make creation and disposal extremely easy. Here is sample code for creating
a queue and sending some binary data using it.

.. JD: Is a dragon_queue_put_close(&put_str); missing here ?

.. code-block:: C
  :linenos:
  :caption: **Creating a Queue using C and the stream interface.**

  #include <dragon/global_types.h>
  #include <dragon/queue.h>

  int main(int argc, char* argv[]) {
    dragonError_t err;
    dragonQueueDescr_t queue;
    dragonQueuePutStream_t put_str;
    uint64_t my_data[VERY_BIG]; /* assumed initialized */
    int val = 100;

    err = dragon_managed_queue_create("my_unique_queue_name", 100, true, NULL, NULL, NULL, &queue);

    if (err != DRAGON_SUCCESS) {
      fprintf(stderr, "The managed queue could not be created. Error Code=%lu\n", err);
      return 1;
    }

    err = dragon_queue_put_open(&queue, &put_str);
    if (err != DRAGON_SUCCESS) {
      fprintf(stderr, "The queue putstream could not be opened. Error Code=%lu\n", err);
      return 1;
    }

    /* some work is done and results need to be shared */

    err = dragon_queue_put_write(&put_str, my_data, VERY_BIG*sizeof(uint64_t), NULL);
    if (err != DRAGON_SUCCESS) {
      fprintf(stderr, "The queue putstream write failed. Error Code=%lu\n", err);
      return 1;
    }

    err = dragon_queue_put_write(&put_str, &val, sizeof(int), NULL);
    if (err != DRAGON_SUCCESS) {
      fprintf(stderr, "The queue putstream write failed. Error Code=%lu\n", err);
      return 1;
    }

    return 0;
  }

Using a Queue with C++
^^^^^^^^^^^^^^^^^^^^^^

Both reading and writing to a Queue in C++ is handled via the std::streambuf
protocol. An istream can be constructed in a similar way to read data from a
stream buffer. The left and right shift operators can be defined for user-defined
data types so arbitrarily complex data can be read and written in this fashion.

.. code-block:: c++
  :linenos:
  :caption: **Creating a C++ Stream over a Queue**

  std::string greeting = "Hello World";
  int id = 42;
  char action = 'a';

  DragonManagedQueueBuf strm_buf("holly", 1024, 1024, true, NULL, NULL);
  std::ostream out(strm_buf, 1024);

  out << greeting << id << action << std::endl;

  out.close()


Using a Queue with Fortran
^^^^^^^^^^^^^^^^^^^^^^^^^^


TBD