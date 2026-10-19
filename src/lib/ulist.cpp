#include <vector>
#include <dragon/return_codes.h>
#include "ulist.h"
#include "err.h"
#include <stdexcept>


#define LOCK_KIND DRAGON_LOCK_FIFO_LITE

#define __lock_list(dlist, locking) ({\
    if (locking) {\
        dragonError_t derr = _lock_list(dlist);\
        if (derr != DRAGON_SUCCESS)\
            append_err_return(derr,"Cannot lock dlist.");\
    }\
})

#define __unlock_list(dlist, locking) ({\
    if (locking) {\
        dragonError_t derr = _unlock_list(dlist);\
        if (derr != DRAGON_SUCCESS)\
            append_err_return(derr,"Cannot unlock dlist.");\
    }\
})

static dragonError_t
_lock_list(dragonList_t * dlist)
{
    dragonError_t lerr = dragon_lock(&dlist->_dlock);
    if (lerr != DRAGON_SUCCESS)
        append_err_return(lerr,"Cannot lock dlist");

    no_err_return(DRAGON_SUCCESS);
}

static dragonError_t
_unlock_list(dragonList_t * dlist)
{
    dragonError_t lerr = dragon_unlock(&dlist->_dlock);
    if (lerr != DRAGON_SUCCESS)
        append_err_return(lerr,"Cannot unlock dlist");

    no_err_return(DRAGON_SUCCESS);
}

class dragonList
{
   public:
    dragonList()
    {
        current_idx = 0;
    }

    ~dragonList()
    {
        dList.clear();
    }

    void addItem(const void * item)
    {
        dList.push_back(item);
    }

    bool delItem(const void * item)
    {
        int size = dList.size();
        for (int k=0;k<size;k++)
            if (item == dList[k]) {
                if (size > 1)
                    dList[k] = dList[size-1];
                dList.pop_back();
                if (current_idx == dList.size())
                    current_idx = 0;
                return true;
            }

        return false;
    }

    const void * getCurrentAdvance()
    {
        const void* current = NULL;
        size_t size = dList.size();

        if (size == 0)
             return NULL;

        current = dList[current_idx];
        current_idx = (current_idx + 1) % size;

        return current;
    }

    const void * getByIdx(int idx)
    {
        size_t size = dList.size();

        if (size == 0 || (size_t) idx > size - 1)
             return NULL;

        return dList[idx];
    }

    bool contains(const void * item)
    {
        for (size_t k=0;k<dList.size();k++)
            if (item == dList[k])
                return true;

        return false;
    }

    size_t size()
    {
        return dList.size();
    }

   private:
    std::vector<const void *> dList;
    size_t current_idx;
};

dragonError_t
dragon_ulist_create(dragonList_t **dlist_in)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"Bad dlist handle.");

    dragonList * cpp_list;
    cpp_list = new dragonList();
    dlist->_list = (void *)cpp_list;

    size_t lock_size = dragon_lock_size(LOCK_KIND);
    dlist->_lmem = calloc(sizeof(char) * lock_size, 1);
    if (dlist->_lmem == NULL) {
        delete static_cast<dragonList *>(dlist->_list);
        err_return(DRAGON_INTERNAL_MALLOC_FAIL,"dlist malloc failed - out of heap space.");
    }

    dragonError_t lerr = dragon_lock_init(&dlist->_dlock, dlist->_lmem, LOCK_KIND);
    if (lerr != DRAGON_SUCCESS) {
        delete static_cast<dragonList *>(dlist->_list);
        free(dlist->_lmem);
        append_err_return(lerr,"Unable to initialize dlist lock.");
    }

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ulist_destroy(dragonList_t **dlist_in)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot destroy it.");

    delete static_cast<dragonList *>(dlist->_list);

    dragonError_t lerr = dragon_lock_destroy(&dlist->_dlock);
    if (lerr != DRAGON_SUCCESS)
        append_err_return(lerr,"Unable to destroy dlist lock.");
    free(dlist->_lmem);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ulist_lock(dragonList_t **dlist_in)
{

    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot lock list.");

    __lock_list(dlist, true);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ulist_unlock(dragonList_t **dlist_in)
{

    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot unlock list.");

    __unlock_list(dlist, true);

    no_err_return(DRAGON_SUCCESS);
}


dragonError_t
dragon_ulist_additem(dragonList_t **dlist_in, const void *item, bool locking)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot add item.");

    dragonList * cpp_list;
    cpp_list = static_cast<dragonList *>(dlist->_list);

    __lock_list(dlist, locking);
    cpp_list->addItem(item);
    __unlock_list(dlist, locking);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ulist_delitem(dragonList_t **dlist_in, const void *item, bool locking)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot delete the item.");

    dragonList * cpp_list;
    cpp_list = static_cast<dragonList *>(dlist->_list);

    __lock_list(dlist, locking);
    bool found = cpp_list->delItem(item);
    __unlock_list(dlist, locking);

    if (!found)
        err_return(DRAGON_NOT_FOUND, "Did not find item in ulist to delete");

    no_err_return(DRAGON_SUCCESS);
}

bool
dragon_ulist_contains(dragonList_t **dlist_in, const void *item, bool locking)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        return false;

    dragonList * cpp_list;
    cpp_list = static_cast<dragonList *>(dlist->_list);

    __lock_list(dlist, locking);
    bool answer = cpp_list->contains(item);
    __unlock_list(dlist, locking);

    return answer;
}

dragonError_t
dragon_ulist_get_current_advance(dragonList_t **dlist_in, void **item, bool locking)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot get item and advance.");

    if (item == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The item pointer is NULL. Cannot return item to NULL pointer.");

    dragonList * cpp_list;
    cpp_list = static_cast<dragonList *>(dlist->_list);

    __lock_list(dlist, locking);
    *item = (void*) cpp_list->getCurrentAdvance();
    __unlock_list(dlist, locking);

    no_err_return(DRAGON_SUCCESS);
}

dragonError_t
dragon_ulist_get_by_idx(dragonList_t **dlist_in, int idx, void **item, bool locking)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot get item.");

    if (item == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The item pointer is NULL. Cannot return item to NULL pointer.");

    if (idx < 0)
        err_return(DRAGON_INVALID_ARGUMENT,"The index is invalid.");

    dragonList * cpp_list;
    cpp_list = static_cast<dragonList *>(dlist->_list);

    __lock_list(dlist, locking);
    *item = (void*) cpp_list->getByIdx(idx);
    __unlock_list(dlist, locking);

    no_err_return(DRAGON_SUCCESS);
}

size_t
dragon_ulist_get_size(dragonList_t **dlist_in, bool locking)
{
    dragonList_t *dlist = *dlist_in;

    if (dlist == NULL)
        err_return(DRAGON_INVALID_ARGUMENT,"The dlist handle is NULL. Cannot get item and advance.");

    dragonList * cpp_list;
    cpp_list = static_cast<dragonList *>(dlist->_list);

    __lock_list(dlist, locking);
    size_t size = cpp_list->size();
    __unlock_list(dlist, locking);

    return size;
}

