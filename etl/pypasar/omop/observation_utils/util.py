from functools import wraps
import time
import logging
logger = logging.getLogger(__name__)


def mapping_wrapper(method):
    '''Helper wrapper for mapping functions to time function and print sample'''
    @wraps(method)
    def wrapped(*args, **kwargs):
        start = time.process_time()

        # Run original function
        return_value = method(*args, **kwargs)

        logger.debug(return_value.sample(5).sort_index())
        logger.info(
            f"Time taken for {method.__name__}: {time.process_time() - start}")
        return return_value
    return wrapped
