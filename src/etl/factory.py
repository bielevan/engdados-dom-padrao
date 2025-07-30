from etl.extract import ExtractGlueCatalogTable, Extract
from pyspark.sql import SparkSession


class ETLFactory:
    """Factory for creating ETL components"""
    
    
    @staticmethod
    def create_extractor(extractor_type: str, spark: SparkSession, config: dict) -> Extract:
        """Create an extractor instance based on type"""
        extractors = {
            "gluecatalog": ExtractGlueCatalogTable
        }
        if extractor_type not in extractors:
            raise ValueError(f"Unknown extractor type: {extractor_type}")
        return extractors[extractor_type](spark, config)


    @staticmethod
    def create_transformer(transformer_type: str, spark: SparkSession, config: dict) -> Transform:
        """Create an transformer instance based on type"""
        transformers = {
            "pdv": "",
            "estab": "",
            "client": "",
            "pdv_emps": ""
        }
        if transformer_type not in transformers:
            raise ValueError(f"Unknown transformer type: {transformer_type}")
        return transformers[transformer_type](spark, config)
