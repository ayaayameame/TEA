import os
from pathlib import Path
import logging

# ==========================================
# 1. Dynamic Path Resolution
# ==========================================
# Get the absolute path of the current file and go up two levels to the project root (TEA)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Dynamically construct paths for core directories
DATA_DIR = PROJECT_ROOT / 'data'
LOGS_DIR = PROJECT_ROOT / 'logs'
LOCALES_DIR = DATA_DIR / 'locales'

# Ensure these directories exist (create them if they don't)
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOCALES_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================
# 2. Database Configuration (SQLite version)
# ==========================================
# The SQLite database file will be stored directly in the data directory
DB_PATH = DATA_DIR / "tarkov_economy.db"

# ==========================================
# 3. Centralized Logger Configuration
# ==========================================
def setup_logger(module_name: str, log_file: str) -> logging.Logger:
    """
    Configures a logger that outputs to both the console and a log file.
    Essential for debugging when the program runs unattended in the background.
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    
    # Define the log output format: Time - Module - Level - Message
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler (writes to the logs directory)
    file_path = LOGS_DIR / log_file
    file_handler = logging.FileHandler(file_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler (displays in the Cursor terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger