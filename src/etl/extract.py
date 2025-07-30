from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class Extract(ABC):


    def __init__(self, spark: SparkSession, config: dict):
        """_summary_

        Args:
            config (dict): _description_
        """
        self.config = config
        self.spark = spark

    
    @abstractmethod
    def extract(self, database: str, table: str, partition_value: str = None, columns: str = "*") -> DataFrame:
        """_summary_

        Args:
            config (dict): _description_

        Returns:
            DataFrame: _description_
        """
        pass



class ExtractGlueCatalogTable(Extract):


    def __init__(self, config):
        super().__init__(config)
    

    @classmethod
    def extract(self, database: str, table: str, partition_value: str = None, columns: str = None) -> DataFrame:
        df = self.spark.read.parquet(f"{database}.{table}")
        if partition_value:
            df.filter(partition_value)
        if columns:
            df.select(columns)
        return df
