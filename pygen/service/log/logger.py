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
