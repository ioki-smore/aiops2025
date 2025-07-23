import os
import logging

def setup_logger(filename: str = 'logs/analysis.log', level: str = 'INFO'):
    log_dir = os.path.dirname(filename)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()  # root logger
    logger.setLevel(getattr(logging, level.upper(), logging.DEBUG))

    # Remove all handlers associated with the root logger (optional, to avoid duplicates)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create handlers
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(filename, mode='w', encoding='utf-8')

    # Create formatter and set it for both handlers
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)