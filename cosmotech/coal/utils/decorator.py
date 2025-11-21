import time
from functools import wraps

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.logger import LOGGER


def timed(operation, debug=False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            process_start = time.time()
            r = func(*args, **kwargs)
            process_time = time.time() - process_start
            log_function = LOGGER.debug if debug else LOGGER.info
            log_function(T("coal.common.timing.operation_completed").format(operation=operation, time=process_time))
            return r

        return wrapper

    return decorator
