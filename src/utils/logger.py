# File: /src/app/utils/logger.py
import logging


def setup_logging(level: str="INFO") -> None:
    """Setup logging configuration"""
    numeric_level = getattr(logging, level.upper(), None)
    
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )