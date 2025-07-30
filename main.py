# File: /src/app/main.py
import argparse
import logging
import os

from utils.config import load_config
from utils.logger import setup_logging
from pipeline import UberEatsPipeline


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="UberEats Data Pipeline")
    
    parser.add_argument(
        "--env",
        type=str,
        default=os.environ.get("ENV", "dev"),
        choices=["dev", "test", "prod"],
        help="Environment to run in"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    
    # Load configuration
    config = load_config(args.env)
    
    # Setup logging
    setup_logging(level=config["app"]["log_level"])
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting pipeline in {args.env} environment")
    
    # Create and run pipeline
    pipeline = UberEatsPipeline(config)
    success = pipeline.run()
    
    if success:
        logger.info("Pipeline completed successfully")
        return 0
    else:
        logger.error("Pipeline execution failed")
        return 1

if __name__ == "__main__":
    exit(main())