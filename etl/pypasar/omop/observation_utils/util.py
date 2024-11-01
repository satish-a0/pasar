from functools import wraps
import time
import logging
logger = logging.getLogger(__name__)


def mapping_wrapper(method):
    '''Helper wrapper for mapping functions to time function and print sample'''
    @wraps(method)
    def wrapped(*args, **kwargs):
        print()
        logger.info(f"Start mapping for {method.__name__}...")
        start = time.process_time()

        # Run original function
        return_value = method(*args, **kwargs)

        # For debugging print out a sample of results
        logger.debug(return_value.dropna().sort_index())
        logger.info(
            f"Time taken: {time.process_time() - start:.3f}s")
        return return_value
    return wrapped
