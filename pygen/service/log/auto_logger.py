import logging
import inspect
import functools
from pygen.service.log.logger import Logger

def auto_log(logger):
    """
    Decorator factory that creates a decorator which logs entry, exit,
    return value, and exceptions for the decorated function using the given logger.

    Args:
        logger (logging.Logger): Logger to use for logging messages.

    Returns:
        Callable: A decorator for functions.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """
            Wrapper function that logs before and after calling the original function.
            """
            logger.debug(f"Entering: {func.__name__} | args={args}, kwargs={kwargs}")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Exiting: {func.__name__} | return={result}")
                return result
            except Exception as e:
                logger.exception(f"Error in {func.__name__}: {e}")
                raise
        return wrapper
    return decorator

class AutoLogMixin(Logger):
    """
    Mixin that automatically decorates all public instance methods of a class
    with logging (entry, exit, exceptions) if the instance has a 'logger' attribute.
    """
    def __init__(self, *args, **kwargs):
        """
        Initialize the mixin by decorating methods of the instance that qualify.

        This should be called via super() in classes that include this mixin.
        """
        super().__init__(*args, **kwargs)

        if hasattr(self, 'logger') and isinstance(self.logger, logging.Logger):
            # Iterate over all bound methods on the instance
            for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
                # Only wrap public methods
                if not name.startswith("_") and callable(method):
                    wrapped = auto_log(self.logger)(method)
                    setattr(self, name, wrapped)