# File: /src/app/core/session.py
from functools import lru_cache
from pyspark.sql import SparkSession


class SparkSessionManager:
    """Singleton manager for Spark sessions"""
    

    @staticmethod
    @lru_cache(maxsize=1)
    def get_session(config: dict) -> SparkSession:
        """Get or create a Spark session"""
        builder = SparkSession.builder.appName(config["app"]["name"])
        # Apply Spark configurations
        for key, value in config["spark"].items():
            if key == "master":
                builder = builder.master(value)
            else:
                builder = builder.config(f"spark.{key}", value)
        return builder.getOrCreate()
    

    @staticmethod
    def stop_session():
        """Stop the current Spark session"""
        SparkSession.getActiveSession().stop()