import logging
from typing import Dict, Optional
from src.core.pipeline import Pipeline
from src.core.session import SparkSessionManager
from src.etl.factory import ETLFactory
from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class PontoVendaPipeline(Pipeline):
    """PontoVenda data processing pipeline"""
    

    def __init__(self, config: dict, eventbridge: bool = None):
        """Initialize the pipeline"""
        super().__init__(config)
        self.spark = SparkSessionManager.get_session(config)
        
        # Pipeline-specific configuration
        self.pipeline_config = {
            **config,
            "columns_pv": [
                "num_pdv AS num_pdv",
                "CAST(LPAD(TRIM(CAST(NUM_CAD_GRL_CTRI_ESTB as STRING)), 14, '0') as BIGINT) AS CNPJ14",
                "CAST(SUBSTR(TRIM(LPAD(CAST(PDV.NUM_CAD_GRL_CTRI_ESTB as STRING), 14, '0')), 1, 8) AS INT) AS CNPJBAS",
                "TRIM(PDV.COD_CNAE) AS COD_CNAE",
                "CAST(PDV.NUM_CAD_GRL_CTRI_ESTB as STRING) AS NUM_CAD_GRL_CTRI_ESTB",
                "PDV.COD_MOT_CAN AS COD_MOT_CAN",
                "PDV.COD_RAM_ATVD AS COD_RAM_ATVD",
                "PDV.COD_GRU_RAM AS COD_GRU_RAM",
                "CAST(PDV.DAT_CDST_PDV as STRING) AS DAT_CDST_PDV",
                "COALESCE(CAST(REPLACE(SUBSTR(CAST(PDV.DAT_CAN_PDV AS STRING), 1, 10), '-', '') AS INT), 0) AS DAT_CAN_PDV",
                "COALESCE(CAST(REPLACE(SUBSTR(CAST(PDV.DAT_RTVC_PDV AS STRING), 1, 10), '-', '') AS INT), 0) AS DAT_RTVC_PDV",
                "CAST((PDV.ANO || LPAD(PDV.MES, 2, '0') || LPAD(PDV.DIA, 2, '0')) AS INT) AS DAT_UPDT",
                "CAST((PDV.ANO || LPAD(PDV.MES, 2, '0') || LPAD(PDV.DIA, 2, '0')) AS INT) AS ANOMESDIA"
            ],
            "columns_estab": [
                "NUM_CAD_GRL_CTRI_ESTB AS CNPJ14",
                "COD_SGL_UF AS UF",
                "TRIM(NOM_CID_ESTB) AS CIDADE",
                "CAST(COD_CEP_ESTB AS INT) AS COD_CEP"
            ],
            "columns_client": [
                "cnpj AS numero_cnpj_completo",
                "id_cnpj_eq3 AS numero_unico_cliente",
                "CONCAT(numero_agencia, '_', numero_conta) AS agencia_conta",
                "canal",
                "data_aceite_ressegmentacao",
                "data_abertura_conta AS data_inicio_relacionamento_bancario",
                "data_encerramento_conta"
            ]
        }
        
        if not eventbridge:
            self.pipeline_config = {
                **self.pipeline_config,
                "reprocess": False,
                "partition_pv": f"{self.config["table_pv_partition"]} <= {self.config["execute_date"]}",
                "partition_client": f"{self.config["table_client_partition"]} >= 20240314",
            }
        else:
            self.pipeline_config = {
                **self.pipeline_config,
                "reprocess": False,
                "partition_pv": f"{self.config["table_pv_partition"]} <= {self.config["execute_date"]}",
                "partition_client": f"{self.config["table_client_partition"]} >= 20240314",
            }


    @classmethod
    def extract(self) -> Dict[Optional[DataFrame]]:
        """Extract data from source"""
        extractor = ETLFactory.create_extractor(
            extractor_type="gluecatalog", 
            spark=self.spark, 
            config=self.pipeline_config
        )

        # Ponto de Venda
        df_ponto_venda: DataFrame = extractor.extract(
            database=self.pipeline_config["database_pv"], 
            table=self.pipeline_config["table_pv"], 
            partition_value=self.pipeline_config["partition_pv"], 
            columns=self.pipeline_config["columns_pv"]
        )
        logger.info(f"Extracted Ponto Venda Table")

        # Clientes EMPS
        df_client: DataFrame = extractor.extract(
            database=self.pipeline_config["database_client"], 
            table=self.pipeline_config["table_client"],
            columns=self.pipeline_config["columns_client"],
            partition_value=self.pipeline_config["partition_client"]
        )
        logger.info(f"Extracted Client Table")

        # Estabelecimento
        df_estab: DataFrame = extractor.extract(
            database=self.pipeline_config["database_estab"], 
            table=self.pipeline_config["table_estab"], 
            column=self.pipeline_config["columns_estab"]
        )
        logger.info(f"Extracted Estabelecimento Table")
        
        return {
            "pontovenda": df_ponto_venda,
            "clientes": df_client,
            "estabelecimento": df_estab
        }
    

    @classmethod
    def transform(self, datas: Dict[str, Optional[DataFrame]]) -> DataFrame:
        """Transform the data"""
        transformer_pdv = ETLFactory.create_transformer(
            transformer_type="pdv",
            spark=self.spark, 
            config=self.pipeline_config
        )
        transformer_client = ETLFactory.create_transformer(
            transformer_type="client", 
            spark=self.spark, 
            config=self.pipeline_config
        )
        transformer_estab = ETLFactory.create_transformer(
            transformer_type="estab", 
            spark=self.spark, 
            config=self.pipeline_config
        )

        transformed_pdv: DataFrame = transformer_pdv.transform(data=datas["pontovenda"])
        logging.info("Transformed Ponto Venda")
        
        transformed_client: DataFrame = transformer_client.transform(data=datas["clientes"])
        logging.info("Transformed Client")

        transformed_estab: DataFrame = transformer_estab.transform(data=datas["estabelecimento"])
        logging.info("Transformed Estab")

        transformed_data: DataFrame = transformer_pdv.transform(datas={
            "pontovenda": transformed_pdv,
            "clientes": transformed_client,
            "estabelecimento": transformed_estab
        })
        return transformed_data


    @classmethod
    def load(self, data: DataFrame):
        """Load data to destination"""
        loader = ETLFactory.create_loader(
            loader_type="hive", 
            spark=self.spark, 
            config=self.pipeline_config
        )
        loader.load(df=data)
        logger.info(f"Loaded table")
