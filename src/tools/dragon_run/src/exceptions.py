class DragonRunBaseException(Exception):
    pass


class DragonRunSingleNodeUnsupported(DragonRunBaseException):
    pass


class DragonRunNoSupportedWLM(DragonRunBaseException):
    pass


class DragonRunMissingAllocation(DragonRunBaseException):
    pass


class HostException(DragonRunBaseException):
    pass


class RemoteProcessError(HostException):
    def __init__(self, exit_code, error_msg):
        super().__init__(error_msg)
        self.exit_code = exit_code
