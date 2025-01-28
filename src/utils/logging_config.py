import logging
from pathlib import Path
from datetime import datetime

def setup_logging(script_name):
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with date
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"{script_name}_{date_str}.log"
    
    # Configure logging to write to both file and console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # This will continue to print to console
        ]
    ) 