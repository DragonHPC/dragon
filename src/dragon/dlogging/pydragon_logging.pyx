from dragon.dtypes_inc cimport *
from dragon.managed_memory cimport *
import enum


class DragonLoggingError(Exception):

    def __init__(self, lib_err, msg):
        cdef char * errstr = dragon_getlasterrstr()

        self.msg = msg
        self.lib_msg = errstr[:].decode('utf-8')
        lib_err_str = dragon_get_rc_string(lib_err)
        self.lib_err = lib_err_str[:].decode('utf-8')

    def __str__(self):
        msg = self.msg
        if len(self.lib_msg) > 0:
            msg += f"\n*** Additional Info ***\n{self.lib_msg}\n*** End Additional Info ***"
        msg += f"\nDragon Error Code: {self.lib_err}"
        return msg

    @enum.unique
    class Errors(enum.Enum):
        SUCCESS = 0
        FAIL = 1
