import functools
import inspect
import logging
import sys

# ANSI color codes for log levels
LOG_COLORS = {
    'DEBUG': '\033[94m',
    'INFO': '\033[92m',
    'WARNING': '\033[93m',
    'ERROR': '\033[91m',
    'CRITICAL': '\033[95m',
    'RESET': '\033[0m',
}

class ColoredFormatter(logging.Formatter):
    """
    A logging Formatter that injects ANSI color codes into the level name
    based on the log level, then delegates to the standard Formatter.
    """
    def format(self, record):
        """
        Apply color codes to the record's levelname and format the record.

        Args:
            record (logging.LogRecord): The record to be formatted.

        Returns:
            str: The formatted log message with ANSI color codes.
        """
        color = LOG_COLORS.get(record.levelname, '')
        reset = LOG_COLORS['RESET']
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)

class Logger:
    """
    Provides a standardized, colored logger instance with contextual information
    (name) for use across the Genesis platform.
    """
    def __init__(self, name: str = "Genesis", level: str = None):
        """
        Initialize the Logger.

        Args:
            name (str): The name for the logger (appears in each message).
            level (str, optional): Desired log level (e.g., "DEBUG", "INFO").
                Defaults to "INFO" if not provided.
        """
        self.name = name
        self.level = level.upper() if level else "INFO"
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """
        Configure and return a logging.Logger instance with color formatting.

        Returns:
            logging.Logger: The configured logger.
        """
        logger = logging.getLogger(self.name)

        if not logger.handlers:
            logger.setLevel(getattr(logging, self.level, logging.INFO))
            formatter = ColoredFormatter(
                fmt=f"[%(asctime)s] [%(levelname)s] [{self.name}] "
                    f"%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False  # Prevent double logging

        return logger

    def get(self) -> logging.Logger:
        """
        Retrieve the internally configured logger.

        Returns:
            logging.Logger: The Genesis logger instance.
        """
        return self.logger

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
