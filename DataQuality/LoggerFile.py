import logging
import colorlog
from datetime import datetime
import os

def ConfigureLogging(base_log_folder='logs'):
    # Create a folder for today's date
    today_date = datetime.now().strftime('%Y-%m-%d')
    log_folder = os.path.join(base_log_folder, today_date)

    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_file = 'app.log'
    log_path = os.path.join(log_folder, log_file)

    # Create a color formatter
    log_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',  # Set INFO logs to green
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    )

    # Configure file logging
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    # Configure console logging
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)

    # Create and configure the root logger (only for file logging)
    logging.basicConfig(level=logging.INFO, handlers=[file_handler])

    # Create a separate logger for console output
    table_logger = logging.getLogger('tableLogger')
    table_logger.setLevel(logging.INFO)
    table_logger.addHandler(console_handler)

    return table_logger
