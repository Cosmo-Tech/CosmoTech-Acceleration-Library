import time

from functools import wraps

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def timed(operation, debug=False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            process_start = time.time()
            func(*args, **kwargs)
            process_time = time.time() - process_start
            msg = T("coal.common.timing.operation_completed").format(operation=operation, time=process_time)
            if debug:
                LOGGER.debug(msg)
            else:
                LOGGER.info(msg)
        return wrapper
    return decorator
