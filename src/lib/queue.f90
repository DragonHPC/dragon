! #include "dragon_fortran.h"

MODULE Mod_Native_Dragon_Queue

! This is the Dragon Native Queue in Fortran

!    USE Mod_Dragon

   USE, INTRINSIC :: iso_c_binding ! F2003 extension

   IMPLICIT NONE

   PRIVATE

   PUBLIC :: DragonQueue_t

   TYPE :: DragonQueue_t

      PRIVATE

      INTEGER(8), PUBLIC :: maxsize
      INTEGER(8), PUBLIC :: m_uid
      INTEGER(8), PUBLIC :: block_size
      LOGICAL(8), PUBLIC :: joinable
      LOGICAL(8), PUBLIC :: managed

   CONTAINS

      PRIVATE

      PROCEDURE, PUBLIC :: attach
      PROCEDURE, PUBLIC :: attach_by_name
      PROCEDURE, PUBLIC :: attach_by_uid
      PROCEDURE, PUBLIC :: detach
      PROCEDURE, PUBLIC :: destroy
      PROCEDURE, PUBLIC :: serialize

      PROCEDURE, PUBLIC :: put
      PROCEDURE, PUBLIC :: put_nowait
      PROCEDURE, PUBLIC :: get
      PROCEDURE, PUBLIC :: get_nowait
      PROCEDURE, PUBLIC :: task_done
      PROCEDURE, PUBLIC :: full
      PROCEDURE, PUBLIC :: empty
      PROCEDURE, PUBLIC :: close

   END TYPE DragonQueue_t

   INTERFACE DragonQueue_t

      MODULE PROCEDURE constructor

   END INTERFACE DragonQueue_t

CONTAINS

   !> @brief initializes the Dragon Queue object
   !!
   !! A Dragon Native Queue relying on Dragon Channels. The interface resembles Python Multiprocessing.Queues. In particular,
   !! the queue can be initialized as joinable, which allows to join on the completion of a particular item. The usual queue.Empty
   !! and queue.Full exceptions from the Python standard library’s queue module are raised to signal timeouts. This implementation
   !! is intended to be a simple and high performance implementation for use on both single node and multiple nodes.
   !!
   !! @param[in] name         name of the queue
   !!
   !! @param[in] maxsize      sets the upperbound limit on the number of items that can be placed in the queue, defaults to 0
   !!
   !! @param[in] m_uid        The m_uid of the memory pool to use, defaults to _DEF_MUID
   !!
   !! @param[in] block_size   Block size for the underlying channel, defaults to DRAGON_NONE
   !!
   !! @param[in] joinable     If this queue should be joinable, defaults to False
   !!
   !! @param[in] managed      If this queue should be managed by the Dragon runtime, defaults to True
   !!
   FUNCTION constructor(name, maxsize, m_uid, block_size, joinable, managed) RESULT(self)

      TYPE(DragonQueue_t) :: self
      CHARACTER(256), INTENT(IN) :: name
      INTEGER(8), INTENT(INOUT), OPTIONAL :: maxsize, m_uid, block_size
      LOGICAL(8), INTENT(INOUT), OPTIONAL :: joinable, managed

      IF (.NOT. present(maxsize)) maxsize = 100
      IF (.NOT. present(m_uid)) m_uid = -1 ! DRAGON_DEF_MUID
      IF (.NOT. present(block_size)) block_size = -1 !DRAGON_NONE
      IF (.NOT. present(joinable)) joinable = .FALSE.
      IF (.NOT. present(managed)) managed = .TRUE.

   END FUNCTION constructor

   SUBROUTINE destructor(self)
      TYPE(DragonQueue_t) :: self
   END SUBROUTINE destructor

   !> @brief Put a data item to the Queue
   !!
   !! Put an item with the given data to the Queue. A timeout can be specified. Zero timeout will try once to put the
   !! the item and retur a timeout error if it fails. Specifying NULL means an infinite wait to
   !! complete the operation.
   !!
   !! @param ptr is a C pointer to the data to put.
   !!
   !! @param nbytes is the number of bytes to put.
   !!
   !! @param timeout is an  optional C pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
   !!
   !! @return
   !!     DRAGON_SUCCESS or a return code to indicate what problem occurred.
   !!
   SUBROUTINE put(self, ptr, nbytes, timeout)
      TYPE(DragonQueue_t) :: self
      TYPE(C_PTR), INTENT(INOUT) :: ptr
      TYPE(C_PTR), INTENT(INOUT), OPTIONAL :: timeout
      INTEGER(8), INTENT(IN) :: nbytes

      IF (.NOT. present(timeout)) timeout = C_NULL_PTR

   END SUBROUTINE put

   !> @brief Put a data item to the Queue
   !!
   !! Put an item with the given data to the Queue, return immediately.
   !!
   !! @param ptr is a pointer that will be updated to memory allocated on the heap and contains the data.
   !!
   !! @param nbytes is the number of bytes in the returned data.
   !!
   !! @return
   !!     DRAGON_SUCCESS or a return code to indicate what problem occurred.
   !!
   SUBROUTINE put_nowait(self, ptr, nbytes)
      TYPE(DragonQueue_t) :: self
      TYPE(C_PTR), INTENT(INOUT) :: ptr
      INTEGER(8), INTENT(IN) :: nbytes
   END SUBROUTINE put_nowait

   !!* @brief Get a data item from the Queue
   !!
   !! Get an item from the Queue. A timeout can be specified. Zero timeout will try once to put the
   !! the item and retur a timeout error if it fails. Specifying NULL means an infinite wait to
   !! complete the operation.
   !!
   !! @param ptr is a pointer that will be updated to memory allocated on the heap and contains the data.
   !!
   !! @param nbytes is the number of bytes in the returned data.
   !!
   !! @param timeout is an optional pointer to the amount of time to wait to complete. NULL indicates blocking indefinitely.
   !!
   !! @return
   !!     DRAGON_SUCCESS or a return code to indicate what problem occurred.
   !!
   SUBROUTINE get(self, ptr, nbytes, timeout)
      TYPE(DragonQueue_t) :: self
      TYPE(C_PTR), INTENT(INOUT) :: ptr
      TYPE(C_PTR), INTENT(INOUT), OPTIONAL :: timeout
      INTEGER(8), INTENT(IN) :: nbytes

      IF (.NOT. present(timeout)) timeout = C_NULL_PTR

   END SUBROUTINE get

   !> @brief Get a data item from the Queue
   !!
   !! Get an item with the given data from the Queue, return immediately.
   !!
   !! @param ptr is a pointer that will be updated to memory allocated on the heap and contains the data.
   !!
   !! @param nbytes is the number of bytes in the returned data.
   !!
   !! @return
   !!     DRAGON_SUCCESS or a return code to indicate what problem occurred.
   !!
   SUBROUTINE get_nowait(self, ptr, nbytes)
      TYPE(DragonQueue_t) :: self
      TYPE(C_PTR), INTENT(INOUT) :: ptr
      INTEGER(8), INTENT(IN) :: nbytes

   END SUBROUTINE get_nowait

   !> @brief
!!
!!
   SUBROUTINE join(self)
      TYPE(DragonQueue_t) :: self
   END SUBROUTINE join

!> @brief
!!
!!
   SUBROUTINE task_done(self)
      TYPE(DragonQueue_t) :: self
   END SUBROUTINE task_done

!> @brief Return True if the queue is full, False otherwise.
!!
!! Return True if the queue is full, False otherwise.
!!
   FUNCTION full(self) RESULT(state)
      TYPE(DragonQueue_t) :: self
      LOGICAL :: state

   END FUNCTION full

!> @brief Return True if the queue is empty, False otherwise. This might not be reliable
!!
!!
   FUNCTION empty(self) RESULT(state)
      TYPE(DragonQueue_t) :: self
      LOGICAL :: state
   END FUNCTION empty

   !> @brief Close the Dragon Queue
   !!
   !! Indicate that no more data will be put on this queue by the current process.
   !! The refcount of the background channel will be released and the channel might be removed,
   !! if no other processes are holding it.
   !!
   !! @return
   !!     DRAGON_SUCCESS or a return code to indicate what problem occurred.
   !!
   SUBROUTINE close(self)
      TYPE(DragonQueue_t) :: self
   END SUBROUTINE close

!> @brief
!!
!!
   SUBROUTINE detach(self)
      TYPE(DragonQueue_t) :: self
   END SUBROUTINE detach

!> @brief
!!
!!
   SUBROUTINE destroy(self)
      TYPE(DragonQueue_t) :: self
   END SUBROUTINE destroy

!> @brief
!!
!!
   FUNCTION serialize(self) RESULT(sdescr)
      TYPE(DragonQueue_t) :: self
      CHARACTER(128) :: sdescr
   END FUNCTION serialize

!> @brief
!!
!!
   FUNCTION attach_by_name(self, name) RESULT(descr)
      CHARACTER(256), INTENT(IN) :: name
      TYPE(DragonQueue_t) :: self
      CHARACTER(128) :: descr
   END FUNCTION attach_by_name

!> @brief
!!
!!
   FUNCTION attach_by_uid(self, uid) RESULT(descr)
      TYPE(DragonQueue_t) :: self
      INTEGER(8), INTENT(IN) :: uid
      CHARACTER(128) :: descr
   END FUNCTION attach_by_uid

!> @brief
!!
!!
   FUNCTION attach(self) RESULT(descr)
      TYPE(DragonQueue_t) :: self
      CHARACTER(128) :: descr
   END FUNCTION attach




END MODULE Mod_Native_Dragon_Queue
