WITH ponto_venda_status AS (
    SELECT
        pdv.num_pdv as num_pdv,
        -- Maior data de cancelamento do ponto de venda
        MAX(CASE
            WHEN (TRIM(CAST(pdv.DAT_CAN_PDV as varchar)) = '' OR pdv.DAT_CAN_PDV is null
                THEN 0
            ELSE CAST(REPLACE(SUBSTR(CAST(pdv.DAT_CAN_PDV as varchar),1,10), '-','') AS INT)
        END) AS DAT_CAN_PDV,
        -- Maior data de reativação do ponto de venda
        MAX(CASE
            WHEN (TRIM(CAST(pdv.DAT_RTVC_PDV as varchar)) = '' or pdv.DAT_RTVC_PDV is null)
            THEN 0
            ELSE CAST(REPLACE(SUBSTR(CAST(pdv.DAT_RTVC_PDV as varchar),1,10), '-','') AS INT)
        END) as DAT_RTVC_PDV,
        {execute_date} as anomesdia
    FROM {config["database_pv"]}.{config["table_pv"]} as pdv
    WHERE {config["table_pv_partition"]} <= {execute_date}
    GROUP BY num_pdv
),

ponto_venda_history AS (
    SELECT
        PDV.num_pdv AS num_pdv,
        CAST(LPAD(TRIM(CAST(PDV.NUM_CAD_GRL_CTRI_ESTB as varchar)),14,'0') as BIGINT) AS CNPJ14,
        CAST(SUBSTR(TRIM(LPAD(CAST(PDV.NUM_CAD_GRL_CTRI_ESTB as varchar),14,'0')),1,8) AS INT) AS CNPJBAS,
        TRIM(PDV.COD_CNAE) COD_CNAE,
        CAST(PDV.NUM_CAD_GRL_CTRI_ESTB as varchar) as NUM_CAD_GRL_CTRI_ESTB,
        PDV.COD_MOT_CAN as COD_MOT_CAN,
        PDV.COD_RAM_ATVD as COD_RAM_ATVD,
        PDV.COD_GRU_RAM as COD_GRU_RAM,
        CAST(PDV.DAT_CDST_PDV as varchar) as DAT_CDST_PDV,
        COALESCE(CAST(REPLACE(SUBSTR(CAST(PDV.DAT_CAN_PDV as varchar),1,10), '-','') AS INT), 0) as DAT_CAN_PDV,
        COALESCE(CAST(REPLACE(SUBSTR(CAST(PDV.DAT_RTVC_PDV as varchar),1,10), '-','') AS INT), 0) as DAT_RTVC_PDV,
        {config["table_pv_partition"]} as DAT_UPDT,
        {execute_date} AS ANOMESDIA
    FROM {config["database_pv"]}.{config["table_pv"]} AS PDV
    WHERE {config["table_pv_partition"]} <= {execute_date}
),

ponto_venda_normalizacao as (
    SELECT
        pdvh.num_pdv,
        pdvh.CNPJ14,
        pdvh.CNPJBAS,
        pdvh.COD_MOT_CAN,
        pdvh.COD_RAM_ATVD,
        pdvh.COD_CNAE,
        pdvh.COD_GRU_RAM,
        CASE
            WHEN pdvh.DAT_CAN_PDV >= pdvs.DAT_CAN_PDV THEN pdvh.DAT_CAN_PDV
            ELSE pdvs.DAT_CAN_PDV
        END AS DAT_CAN_PDV,
        CASE
            WHEN pdvh.DAT_RTVC_PDV >= pdvs.DAT_RTVC_PDV THEN pdvh.DAT_RTVC_PDV
            ELSE pdvs.DAT_RTVC_PDV
        END AS DAT_RTVC_PDV,
        pdvh.DAT_CDST_PDV,
        pdvh.DAT_UPDT as DAT_UPDT,
        ROW_NUMBER() OVER(
            PARTITION BY pdvh.num_pdv
            ORDER BY pdvh.ANOMESDIA DESC
        ) AS INDEX_NUM,
        pdvh.ANOMESDIA AS anomesdia
    FROM ponto_venda_history AS pdvh
    INNER JOIN ponto_venda_status AS pdvs ON (
        pdvh.num_pdv = pdvs.num_pdv 
        AND pdvs.anomesdia = {execute_date}
    )
),

estabelecimentos AS (
    SELECT
        CNPJ14,
        UF,
        CIDADE,
        COD_CEP
    FROM (
        SELECT
            NUM_CAD_GRL_CTRI_ESTB AS CNPJ14,
            COD_SGL_UF AS UF,
            TRIM(NOM_CID_ESTB) AS CIDADE,
            CAST(COD_CEP_ESTB as int) AS COD_CEP,
            ROW_NUMBER() OVER (
                PARTITION BY NUM_CAD_GRL_CTRI_ESTB
                ORDER BY {config["table_estab_partition"]} DESC
            ) AS index_est
        FROM {config["database_estab"]}.{config["table_estab"]}
    )
    WHERE index_est = 1
),

clientes_emps AS (
    WITH sot_contas AS (
        SELECT
            cnpj AS numero_cnpj_completo,
            id_cnpj_eq3 AS numero_unico_cliente,
            CONCAT(numero_agencia,'_',numero_conta) AS agencia_conta,
            canal,
            data_aceite_ressegmentacao,
            data_abertura_conta AS data_inicio_relacionamento_bancario,
            data_encerramento_conta
        FROM {config["database_client"]}.{config["table_client"]}
        WHERE anomesdia = {execute_date}
        AND CONCAT(numero_agencia,'_',numero_conta) NOT IN (
            '4340_0099209', '4340_0099223', '4340_0099217', '0045_0097934', '8850_0098786', '6083_0098321', '7379_0099408', '0096_0039573',
            '8907_0026611', '8907_0031013', '1589_0036063', '5876_0099833', '7243_0017269', '1433_0055501', '7940_0003018', '6255_0023544',
            '9278_0069372', '9278_0060034', '9278_0070313', '9278_0070315', '1185_0039663', '0566_0099412', '7147_0049258', '0585_0045023',
            '7143_0016240', '0071_0098436', '6277_0099116', '4271_0030345', '8170_0028101', '8170_0027925', '8170_0028576', '8170_0028575',
            '1628_0047283', '0387_0098998',
            '6321_0099559', '0942_0098666', '0007_0099076',
            '6396_0099576', '6753_0099885',
            '1616_0099122', '0454_0099443'
        )
    ),
    
    resseg_out AS (
        SELECT DISTINCT CONCAT(numero_agencia,'_',numero_conta) AS agencia_conta
        FROM {config["database_client"]}.{config["table_client"]}
        WHERE canal = 'resseg out'
        AND CONCAT(numero_agencia,'_',numero_conta) NOT IN (
            '4340_0099209', '4340_0099223', '4340_0099217', '0045_0097934', '8850_0098786', '6083_0098321', '7379_0099408',
            '0096_0039573', '8907_0026611', '8907_0031013', '1589_0036063', '5876_0099833', '7243_0017269', '1433_0055501',
            '7940_0003018', '6255_0023544', '9278_0069372', '9278_0060034', '9278_0070313', '9278_0070315', '1185_0039663',
            '0566_0099412', '7147_0049258', '0585_0045023', '7143_0016240', '0071_0098436', '6277_0099116', '4271_0030345',
            '8170_0028101', '8170_0027925', '8170_0028576', '8170_0028575', '1628_0047283', '0387_0098998',
            '6321_0099559', '0942_0098666', '0007_0099076',
            '6396_0099576', '6753_0099885',
            '1616_0099122', '0454_0099443'
        )
    ),
    
    historico_cli_resseg_out AS (
        SELECT DISTINCT
            a.cnpj AS numero_cnpj_completo,
            a.id_cnpj_eq3 AS numero_unico_cliente,
            CONCAT(a.numero_agencia,'_',a.numero_conta) AS agencia_conta,
            a.canal,
            CASE
                WHEN a.canal IN ('resseg in', 'resseg out') THEN a.data_aceite_ressegmentacao
                ELSE a.data_abertura_conta
            END AS data_aceite_ressegmentacao,
            a.data_abertura_conta AS data_inicio_relacionamento_bancario
        FROM {config["database_client"]}.{config["table_client"]} a
        WHERE EXISTS (SELECT 1 FROM resseg_out WHERE CONCAT(a.numero_agencia,'_',a.numero_conta) = resseg_out.agencia_conta)
    ),
    
    resseg_in_resseg_out AS (
        SELECT 
            numero_cnpj_completo,
            numero_unico_cliente,
            agencia_conta,
            canal,
            data_aceite_ressegmentacao,
            data_inicio_relacionamento_bancario,
            ROW_NUMBER() OVER (PARTITION BY agencia_conta, CASE WHEN canal = 'resseg out' THEN 'resseg out' ELSE 'entrada' END ORDER BY data_aceite_ressegmentacao ASC) AS rank_cli
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
        
        UNION ALL
        
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
                WHEN agencia_conta IN ('3184_0099691', '9164_0099890', '7210_0099675', '5171_0099891') THEN 'outros'
                WHEN agencia_conta IN ('4039_0099799', '7669_0099301', '4275_0098736', '7489_0099160', '1412_0097890', '7332_0099664', '1393_0099325', '6110_0098319', '5516_0099827') THEN 'onboarding'
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
                WHEN jornada IN ('outros','outro') THEN 'one_itau'
                ELSE jornada 
            END) AS jornada,
            (CASE
                WHEN jornada = 'resseg in' THEN data_aceite_ressegmentacao
                ELSE data_inicio_relacionamento_bancario 
            END) AS data_abertura_conta_emps,
            data_encerramento_conta AS data_encerramento_conta_emps
        FROM ajuste_jornada_casos_pontuais
    )
    
    SELECT DISTINCT
        numero_cnpj_completo,
        numero_unico_cliente,
        agencia_conta,
        jornada,
        data_abertura_conta_emps,
        data_encerramento_conta_emps,
        IF(data_encerramento_conta_emps = DATE '9999-12-31' OR data_encerramento_conta_emps IS NULL, TRUE, FALSE) AS flag_conta_ativa
    FROM schema_spec
),

ponto_venda_emps AS (
    SELECT
        PDV.num_pdv,
        cliente.numero_unico_cliente,
        CAST(cliente.numero_cnpj_completo AS BIGINT) AS CNPJ14,
        PDV.CNPJBAS,
        SUBSTRING(cliente.agencia_conta, 1, 4) AS NUMERO_AGENCIA,
        SUBSTRING(cliente.agencia_conta, 6, 7) AS NUMERO_CONTA,
        PDV.COD_MOT_CAN,
        PDV.COD_GRU_RAM,
        PDV.COD_RAM_ATVD,
        PDV.COD_CNAE,
        PDV_ESTAB.COD_CEP,
        PDV_ESTAB.UF,
        PDV_ESTAB.CIDADE,
        PDV.DAT_CAN_PDV,
        PDV.DAT_CDST_PDV,
        PDV.DAT_RTVC_PDV,
        PDV.DAT_UPDT,
        PDV.ANOMESDIA
    FROM ponto_venda_normalizacao AS PDV
    INNER JOIN clientes_emps AS cliente ON
        CAST(cliente.numero_cnpj_completo AS BIGINT) = pdv.CNPJ14
        AND cliente.flag_conta_ativa = True
    LEFT JOIN estabelecimentos AS PDV_ESTAB ON
        PDV.CNPJ14 = PDV_ESTAB.CNPJ14
    WHERE PDV.INDEX_NUM = 1
)

SELECT
    PDV.num_pdv numero_ponto_venda,
    PDV.NUMERO_UNICO_CLIENTE numero_unico_cliente,
    PDV.CNPJ14 numero_cnpj14,
    PDV.CNPJBAS numero_cnpj_base,
    PDV.NUMERO_AGENCIA numero_agencia,
    PDV.NUMERO_CONTA numero_conta,
    PDV.COD_MOT_CAN codigo_motivo_cancelamento,
    PDV.COD_GRU_RAM codigo_grupo_ramo,
    PDV.COD_RAM_ATVD codigo_ramo_atividade,
    PDV.COD_CNAE codigo_cnae,
    PDV.COD_CEP codigo_cep,
    PDV.UF sigla_unidade_federativa,
    PDV.CIDADE nome_cidade_estabelecimento,
    PDV.DAT_CAN_PDV data_cancelamento_ponto_venda,
    PDV.DAT_CDST_PDV data_cadastro_ponto_venda,
    PDV.DAT_RTVC_PDV data_reativacao_ponto_venda,
    PDV.DAT_UPDT data_atualizacao_ponto_venda,
    anomesdia
FROM ponto_venda_emps PDV
WHERE (
    COALESCE(DAT_CAN_PDV, 0) = 0
    OR DAT_CAN_PDV >= 20231001
    OR DAT_RTVC_PDV >= DAT_CAN_PDV
)