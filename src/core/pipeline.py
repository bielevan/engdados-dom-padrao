import logging
from typing import Dict, Optional
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class Pipeline(ABC):
    

    def __init__(self, config: dict):
        self.config = config
        logger.info(f"Initialized pipeline with configs {config}")


    @abstractmethod
    def extract(self) -> Dict[str, DataFrame]:
        """_summary_

        Returns:
            pd.DataFrame: _description_
        """
        pass


    @abstractmethod
    def transform(self, datas: Dict[str, Optional[DataFrame]]) -> Dict[Optional[DataFrame]]:
        """_summary_

        Args:
            data (pd.DataFrame): _description_

        Returns:
            any: _description_
        """
        pass


    @abstractmethod
    def load(self, data: DataFrame):
        """_summary_

        Args:
            df (pd.DataFrame): _description_
        """
        pass


    def run(self) -> None:
        """_summary_
        """
        try:
            logger.info("Starting extraction phase")
            extracted_data: Dict[str, Optional[DataFrame]] = self.extract()

            logger.info("Starting transformation phase")
            transformed_data: DataFrame = self.transform(datas=extracted_data)

            logger.info("Starting loading phase")
            self.load(data=transformed_data)
            
            logger.info("Pipeline execution completed successfully")
            return True
        except Exception as e:
            logger.error("Error by executing run lambda function")
            logger.error(e)
            raise e