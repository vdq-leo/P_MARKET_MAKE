import os
import sys
sys.path.append(os.getcwd())

import logging
from logging.handlers import TimedRotatingFileHandler

# Create a logger directory if it doesn't exist
LOG_DIR = "logger"
os.makedirs(LOG_DIR, exist_ok=True)

FORMAT = '[%(asctime)-15s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
loggers = {}

class SizeAndTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Custom handler for rotating log files based on time and size."""
    
    def __init__(self, filename: str, when: str = 'midnight', interval: int = 1,
                 backupCount: int = 2, maxBytes: int = 5 * 1024 * 1024,
                 encoding: str = None, delay: bool = False, utc: bool = False, atTime: str = None):
        super().__init__(filename, when=when, interval=interval, backupCount=backupCount,
                         encoding=encoding, delay=delay, utc=utc, atTime=atTime)
        self.maxBytes = maxBytes

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        """Determine whether to rollover the log file."""
        if super().shouldRollover(record):
            return True
        
        if self.stream is None:
            self.stream = self._open()
        
        if os.stat(self.baseFilename).st_size >= self.maxBytes:
            return True
        
        return False

def setup_logger(name: str, log_file: str, level: int = logging.DEBUG) -> logging.Logger:
    """Set up a logger with the given name and log file."""
    logger_name = f"{os.path.basename(os.getcwd())}_{name}"
    
    if logger_name in loggers:
        return loggers[logger_name]

    formatter = logging.Formatter(FORMAT)
    handler = SizeAndTimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        maxBytes=100 * 1024 * 1024,
        backupCount=2
    )
    
    handler.setFormatter(formatter)
    handler.setLevel(level)
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.addHandler(handler)
    loggers[logger_name] = logger
    logger.propagate = False
    
    return logger

def setup_logger_global(name: str, log_file: str, level: int = logging.DEBUG) -> logging.Logger:
    """Setup a global logger with the given name and log file."""
    full_log_path = os.path.join('./logger', log_file)
    return setup_logger(name, full_log_path, level)