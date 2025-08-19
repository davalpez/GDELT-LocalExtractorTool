import logging
import sys

def setup_logging():
    """
    Configures the root logger for the entire application.

    Subsequent calls to logging.getLogger(__name__) in other modules
    will inherit this configuration.
    """
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)  # Set the lowest level of messages to handle

    # Create a handler to write logs to the console (standard output)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO) 
    
    # Create a formatter to define the log message's structure
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)

    # Add the handler to the root logger
    # Avoid adding handlers multiple times
    if not root_logger.handlers:
        root_logger.addHandler(handler)

    # Specific logger configurations
    # ------------------------------------------------------------------

    logging.getLogger('utils.spark_transforms').setLevel(logging.ERROR)

    logging.getLogger('requests').setLevel(logging.WARNING)

    logging.getLogger('urllib3').setLevel(logging.WARNING)
    # ------------------------------------------------------------------