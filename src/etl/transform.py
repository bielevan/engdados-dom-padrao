from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class Transform(ABC):


    def __init__(self, spark: SparkSession, config: dict):
        """_summary_

        Args:
            spark (SparkSession): _description_
            config (dict): _description_
        """
        self.spark = spark
        self.config = config


    @abstractmethod
    def transform(self, data: DataFrame) -> DataFrame:
        """_summary_

        Args:
            spark (SparkSession): _description_
            data (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """
        pass

    
    @classmethod
    def createTempViewFromDF(self, data: DataFrame, name: str) -> DataFrame:
        """_summary_

        Args:
            seld (_type_): _description_
            data (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """
        data.createOrReplaceTempView(name)
        

class TransformEstab(Transform):

    
    def __init__(self, spark, config):
        super().__init__(spark, config)


    @classmethod
    def transform(self, data: DataFrame) -> DataFrame:
        """_summary_

        Args:
            data (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """
        self.createTempViewFromDF(data=data, name="S_ESTB")
        df = self.spark.sql("""
            SELECT
                CNPJ14,
                UF,
                CIDADE,
                COD_CEP
            FROM (
                SELECT
                    CNPJ14,
                    UF,
                    CIDADE,
                    COD_CEP,
                    ROW_NUMBER() OVER (
                        PARTITION BY CNPJ14
                        ORDER BY CAST((ano || LPAD(mes, 2, '0') || LPAD(dia, 2, '0')) AS INT) DESC
                    ) AS index_est
                FROM S_ESTB
            )
            WHERE index_est = 1
        """)
        return df


class TransformClient(Transform):

    
    def __init__(self, spark: SparkSession, config: dict):
        super().__init__(spark, config)


    @classmethod
    def transform(self, data: DataFrame) -> DataFrame:
        """_summary_

        Args:
            data (DataFrame): _description_

        Returns:
            DataFrame: _description_
        """
        self.createTempViewFromDF(data=data, name="tb_hr5_ext_clientes_emps_sot")
        df = self.spark.sql("""
            WITH sot_contas AS (
                SELECT
                    cnpj        AS numero_cnpj_completo,
                    id_cnpj_eq3 AS numero_unico_cliente,
                    CONCAT(numero_agencia, '_', numero_conta) AS agencia_conta,
                    canal,
                    data_aceite_ressegmentacao,
                    data_abertura_conta AS data_inicio_relacionamento_bancario,
                    data_encerramento_conta
                FROM tb_hr5_ext_clientes_emps_sot
                AND CONCAT(numero_agencia, '_', numero_conta) NOT IN (
                    -- contas não emps
                    '4340_0099209', '4340_0099223', '4340_0099217', '0045_0097934', '8850_0098786', '6083_0098321', '7379_0099408', '0096_0039573',
                    '8907_0026611', '8907_0031013', '1589_0036063', '5876_0099833', '7243_0017269', '1433_0055501', '7940_0003018', '6255_0023544',
                    '9278_0069372', '9278_0060034', '9278_0070313', '9278_0070315', '1185_0039663', '0566_0099412', '7147_0049258', '0585_0045023',
                    '7143_0016240', '0071_0098436', '6277_0099116', '4271_0030345', '8170_0028101', '8170_0027925', '8170_0028576', '8170_0028575',
                    '1628_0047283', '0387_0098998',
                    -- contas expiradas
                    '6321_0099559', '0942_0098666', '0007_0099076',
                    -- conta teste credito em 2021
                    '6396_0099576', '6753_0099885',
                    -- caso excecao resseg
                    '1616_0099122', '0454_0099443'
                )
            ),
            
            resseg_out AS (
                SELECT DISTINCT CONCAT(numero_agencia, '_', numero_conta) AS agencia_conta
                FROM "db_corp_canaisnaoassistidos_contaatlas_sot_01"."tb_hr5_ext_clientes_emps_sot"
                WHERE canal = 'resseg out'
                AND CONCAT(numero_agencia, '_', numero_conta) NOT IN (
                    -- contas não emps
                    '4340_0099209', '4340_0099223', '4340_0099217', '0045_0097934', '8850_0098786', '6083_0098321', '7379_0099408',
                    '0096_0039573', '8907_0026611', '8907_0031013', '1589_0036063', '5876_0099833', '7243_0017269', '1433_0055501',
                    '7940_0003018', '6255_0023544', '9278_0069372', '9278_0060034', '9278_0070313', '9278_0070315', '1185_0039663',
                    '0566_0099412', '7147_0049258', '0585_0045023', '7143_0016240', '0071_0098436', '6277_0099116', '4271_0030345',
                    '8170_0028101', '8170_0027925', '8170_0028576', '8170_0028575', '1628_0047283', '0387_0098998',
                    -- contas expiradas
                    '6321_0099559', '0942_0098666', '0007_0099076',
                    -- conta teste credito em 2021
                    '6396_0099576', '6753_0099885',
                    -- caso excecao resseg
                    '1616_0099122', '0454_0099443'
                )
            ),
            
            historico_cli_resseg_out AS (
                SELECT DISTINCT
                    a.cnpj AS numero_cnpj_completo,
                    a.id_cnpj_eq3 AS numero_unico_cliente,
                    CONCAT(a.numero_agencia, '_', a.numero_conta) AS agencia_conta,
                    a.canal,
                    -- Esse case serve para pegar data aceite ressegmentacao caso seja algum resseg ou data da abertura casou seja outro canal
                    CASE
                        WHEN a.canal IN ('resseg in', 'resseg out') THEN a.data_aceite_ressegmentacao
                        ELSE a.data_abertura_conta
                    END AS data_aceite_ressegmentacao,
                    a.data_abertura_conta AS data_inicio_relacionamento_bancario
                FROM "db_corp_canaisnaoassistidos_contaatlas_sot_01"."tb_hr5_ext_clientes_emps_sot" a
                WHERE EXISTS (SELECT 1 FROM resseg_out WHERE CONCAT(a.numero_agencia, '_', a.numero_conta) = resseg_out.agencia_conta)
            ),
            
            resseg_in_resseg_out AS (
                SELECT
                    numero_cnpj_completo,
                    numero_unico_cliente,
                    agencia_conta,
                    canal,
                    data_aceite_ressegmentacao,
                    data_inicio_relacionamento_bancario,
                    -- aqui ele faz o rank baseado em agencia_conta e o tipo se foi resseg_out ou algum tipo de entrada, ordenando por data_aceite_ressegmentacao(lembrando do case criado na cte anterior)
                    ROW_NUMBER() OVER (
                        PARTITION BY agencia_conta, 
                        CASE WHEN canal = 'resseg out' THEN 'resseg out' ELSE 'entrada' END 
                        ORDER BY data_aceite_ressegmentacao ASC
                    ) AS rank_cli
                FROM historico_cli_resseg_out
            ),
            
            base_canal_entrada_ajustado AS (
                SELECT
                    resseg_in.numero_cnpj_completo,
                    resseg_in.numero_unico_cliente,
                    resseg_in.agencia_conta,
                    resseg_in.canal,
                    CAST(resseg_in.data_aceite_ressegmentacao AS DATE) AS data_aceite_ressegmentacao,
                    CAST(resseg_in.data_inicio_relacionamento_bancario AS DATE) AS data_inicio_relacionamento_bancario,
                    CAST(resseg_out.data_aceite_ressegmentacao AS DATE) AS data_encerramento_conta
                FROM (SELECT * FROM resseg_in_resseg_out WHERE canal != 'resseg out') resseg_in
                LEFT JOIN (SELECT * FROM resseg_in_resseg_out WHERE canal = 'resseg out') resseg_out ON
                    resseg_in.agencia_conta = resseg_out.agencia_conta
                    AND resseg_in.rank_cli = resseg_out.rank_cli
                ----
                UNION ALL
                ----
                SELECT
                    numero_cnpj_completo,
                    numero_unico_cliente,
                    agencia_conta,
                    canal,
                    CAST(data_aceite_ressegmentacao AS DATE) AS data_aceite_ressegmentacao,
                    CAST(data_inicio_relacionamento_bancario AS DATE) AS data_inicio_relacionamento_bancario,
                    CAST(data_encerramento_conta AS DATE) AS data_encerramento_conta
                FROM sot_contas a
                WHERE NOT EXISTS (SELECT 1 FROM resseg_out WHERE a.agencia_conta = resseg_out.agencia_conta)
            ),
            
            ajuste_jornada_casos_pontuais AS (
                SELECT
                    numero_cnpj_completo,
                    numero_unico_cliente,
                    agencia_conta,
                    (CASE
                        -- ajuste one_itau
                        WHEN agencia_conta IN (
                            -- na sot = 'resseg in'
                            '3184_0099691',
                            -- na sot = 'onboarding'
                            '9164_0099890',
                            '7210_0099675',
                            '5171_0099891'
                        ) THEN 'outros'
                        WHEN agencia_conta IN (
                            -- sot = 'outros'
                            '4039_0099799',
                            '7669_0099301',
                            '4275_0098736',
                            '7489_0099160',
                            '1412_0097890',
                            '7332_0099664',
                            '1393_0099325',
                            '6110_0098319',
                            '5516_0099827'
                        ) THEN 'onboarding'
                        ELSE canal 
                    END) AS jornada,
                    data_aceite_ressegmentacao,
                    data_inicio_relacionamento_bancario,
                    data_encerramento_conta
                FROM base_canal_entrada_ajustado
            ),
            
            schema_spec AS (
                SELECT DISTINCT
                    numero_cnpj_completo,
                    numero_unico_cliente,
                    agencia_conta,
                    (CASE
                        WHEN jornada = 'resseg in' THEN 'ressegmentacao'
                        WHEN jornada IN ('outros', 'outro') THEN 'one_itau'
                        ELSE jornada 
                    END) AS jornada,
                    (CASE
                        WHEN jornada = 'resseg in' THEN data_aceite_ressegmentacao
                        ELSE data_inicio_relacionamento_bancario 
                    END) AS data_abertura_conta_emps,
                    data_encerramento_conta AS data_encerramento_conta_emps
                FROM ajuste_jornada_casos_pontuais
            )
            SELECT
                numero_cnpj_completo,
                numero_unico_cliente,
                agencia_conta,
                jornada,
                data_abertura_conta_emps,
                data_encerramento_conta_emps,
                IF(data_encerramento_conta_emps = DATE '9999-12-31' OR data_encerramento_conta_emps IS NULL, TRUE, FALSE) AS flag_conta_ativa
            FROM schema_spec
        """)
        return df
    

