from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .batch import Task


class ProxyObj:
    def __init__(self, task: "Task") -> None:
        """
        Initialize a proxy object for the given task. The proxy object represents the
        return value of the task. Most of it's dunder methods are defined to perform
        a batch fence before actually doing the work of the dunder method, since we need
        to make sure that the return value is ready before it's used. If any of the
        arguments to a dunder method are themselves proxy objects, then we replace them
        with their (now available) return values.

        :param task: The task whose return value will be represented by the proxy object.
        :type task: Task

        :return: Returns None.
        :rtype: None
        """
        self._batch = task._batch
        self._result = task.result

        # there's a chicken-and-egg situation with __setattr__ and __getattribute__ being
        # called during __init__, so we need to update them to the real functions here
        self.__getattribute__ = self._proxy_getattribute
        self.__setattr__ = self._proxy_setattr

    def _call_dunder_with_actual_args(self, dunder_name: str, *args, **kwargs) -> Any:
        """
        Performs a fence and calls the dunder method with this object, and all arguments,
        replaced by the actual return values that they represent. The idea is to determine
        the values of all proxy objects lazily to maximize batch sizes, while simultaneously
        providing a programming experience for the user that feels like serial programing.

        :param dunder_name: The name of the dunder method to be called.
        :type dunder_name: str

        :return: Returns whatever the dunder method being called returns.
        :rtype: Any
        """
        # TODO: should we make _background_tasks a dict, and check if this task is
        # in the dict before compiling and starting the tasks?
        self._batch.fence()

        actual_args = []
        actual_kwargs = {}

        for arg in args:
            if isinstance(arg, ProxyObj):
                arg = arg._result.get()
            actual_args.append(arg)

        for key, val in kwargs.items():
            if isinstance(val, ProxyObj):
                val = val._result.get()
            actual_kwargs[key] = val

        actual_obj = self._result.get()
        dunder_method = getattr(actual_obj, dunder_name)

        return dunder_method(*actual_args, **actual_kwargs)

    # TODO: Are __new__ and/or __del__ required?

    def __repr__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__repr__", *args, **kwargs)

    def __str__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__str__", *args, **kwargs)

    def __bytes__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__bytes__", *args, **kwargs)

    def __format__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__format__", *args, **kwargs)

    def __lt__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__lt__", *args, **kwargs)

    def __le__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__le__", *args, **kwargs)

    def __eq__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__eq__", *args, **kwargs)

    def __ne__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ne__", *args, **kwargs)

    def __gt__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__gt__", *args, **kwargs)

    def __ge__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ge__", *args, **kwargs)

    def __hash__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__hash__", *args, **kwargs)

    def __bool__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__bool__", *args, **kwargs)

    def __getattr__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__getattr__", *args, **kwargs)

    def _proxy_getattribute(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__getattribute__", *args, **kwargs)

    def __getattribute__(self, name) -> Any:
        return super().__getattribute__(name)

    def _proxy_setattr(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__setattribute__", *args, **kwargs)

    def __setattr__(self, name, value) -> None:
        super().__setattr__(name, value)

    def __delattr__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__delattr__", *args, **kwargs)

    def __dir__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__dir__", *args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__call__", *args, **kwargs)

    def __len__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__len__", *args, **kwargs)

    def __length_hint__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__length_hint__", *args, **kwargs)

    def __getitem__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__getitem__", *args, **kwargs)

    def __setitem__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__setitem__", *args, **kwargs)

    def __delitem__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__delitem__", *args, **kwargs)

    def __missing__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__missing__", *args, **kwargs)

    def __iter__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__iter__", *args, **kwargs)

    def __reversed__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__reversed__", *args, **kwargs)

    def __contains__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__contains__", *args, **kwargs)

    def __add__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__add__", *args, **kwargs)

    def __sub__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__sub__", *args, **kwargs)

    def __mul__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__mul__", *args, **kwargs)

    def __matmul__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__matmul__", *args, **kwargs)

    def __truediv__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__truediv__", *args, **kwargs)

    def __floordiv__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__floordiv__", *args, **kwargs)

    def __mod__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__mod__", *args, **kwargs)

    def __divmod__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__divmod__", *args, **kwargs)

    def __pow__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__pow__", *args, **kwargs)

    def __lshift__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__lshift__", *args, **kwargs)

    def __rshift__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rshift__", *args, **kwargs)

    def __and__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__and__", *args, **kwargs)

    def __xor__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__xor__", *args, **kwargs)

    def __or__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__or__", *args, **kwargs)

    def __radd__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__radd__", *args, **kwargs)

    def __rsub__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rsub__", *args, **kwargs)

    def __rmul__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rmul__", *args, **kwargs)

    def __rmatmul__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rmatmul__", *args, **kwargs)

    def __rtruediv__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rtruediv__", *args, **kwargs)

    def __rfloordiv__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rfloordiv__", *args, **kwargs)

    def __rmod__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rmod__", *args, **kwargs)

    def __rdivmod__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rdivmod__", *args, **kwargs)

    def __rpow__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rpow__", *args, **kwargs)

    def __rlshift__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rlshift__", *args, **kwargs)

    def __rrshift__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rrshift__", *args, **kwargs)

    def __rand__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rand__", *args, **kwargs)

    def __rxor__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__rxor__", *args, **kwargs)

    def __ror__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ror__", *args, **kwargs)

    def __iadd__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__iadd__", *args, **kwargs)

    def __isub__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__isub__", *args, **kwargs)

    def __imul__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__imul__", *args, **kwargs)

    def __imatmul__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__imatmul__", *args, **kwargs)

    def __itruediv__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__itruediv__", *args, **kwargs)

    def __ifloordiv__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ifloordiv__", *args, **kwargs)

    def __imod__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__imod__", *args, **kwargs)

    def __ipow__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ipow__", *args, **kwargs)

    def __ilshift__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ilshift__", *args, **kwargs)

    def __irshift__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__irshift__", *args, **kwargs)

    def __iand__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__iand__", *args, **kwargs)

    def __ixor__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ixor__", *args, **kwargs)

    def __ior__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ior__", *args, **kwargs)

    def __neg__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__neg__", *args, **kwargs)

    def __pos__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__pos__", *args, **kwargs)

    def __abs__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__abs__", *args, **kwargs)

    def __invert__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__invert__", *args, **kwargs)

    def __complex__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__complex__", *args, **kwargs)

    def __int__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__int__", *args, **kwargs)

    def __float__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__float__", *args, **kwargs)

    def __index__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__index__", *args, **kwargs)

    def __round__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__round__", *args, **kwargs)

    def __trunc__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__trunc__", *args, **kwargs)

    def __floor__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__floor__", *args, **kwargs)

    def __ceil__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__ceil__", *args, **kwargs)

    def __enter__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__enter__", *args, **kwargs)

    def __exit__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__exit__", *args, **kwargs)

    def __prepare__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__prepare__", *args, **kwargs)

    def __instancecheck__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__instancecheck__", *args, **kwargs)

    def __subclasscheck__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__subclasscheck__", *args, **kwargs)

    def __init_subclass__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__init_subclass__", *args, **kwargs)

    def __subclasses__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__subclasses__", *args, **kwargs)

    def __mro_entries__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__mro_entries__", *args, **kwargs)

    def __class_getitem__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__class_getitem__", *args, **kwargs)

    def __set_name__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__set_name__", *args, **kwargs)

    def __get__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__get__", *args, **kwargs)

    def __set__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__set__", *args, **kwargs)

    def __delete__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__delete__", *args, **kwargs)

    def __buffer__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__buffer__", *args, **kwargs)

    def __release_buffer__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__release_buffer__", *args, **kwargs)

    def __await__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__await__", *args, **kwargs)

    def __aenter__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__aenter__", *args, **kwargs)

    def __aexit__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__aexit__", *args, **kwargs)

    def __aiter__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__aiter__", *args, **kwargs)

    def __anext__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__anext__", *args, **kwargs)

    def __post_init__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__post_init__", *args, **kwargs)

    def __replace__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__replace__", *args, **kwargs)

    def __copy__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__copy__", *args, **kwargs)

    def __deepcopy__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__deepcopy__", *args, **kwargs)

    def __getnewargs__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__getnewargs__", *args, **kwargs)

    def __getnewargs_ex__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__getnewargs_ex__", *args, **kwargs)

    def __getstate__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__getstate__", *args, **kwargs)

    def __reduce__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__reduce__", *args, **kwargs)

    def __reduce_ex__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__reduce_ex__", *args, **kwargs)

    def __setstate__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__setstate__", *args, **kwargs)

    def __fspath__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__fspath__", *args, **kwargs)

    def __sizeof__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__sizeof__", *args, **kwargs)

    def __subclasshook__(self, *args, **kwargs):
        return self._call_dunder_with_actual_args("__subclasshook__", *args, **kwargs)
