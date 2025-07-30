class AthenaSQL:


    @staticmethod
    def get_sum_sales_agg_pdv(database: str, table: str) -> str:
        return f"""
        SELECT
            numero_ponto_venda,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', 0, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_0,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -1, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_1,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -2, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_2,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -3, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_3,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -4, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_4,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -5, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_5,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -6, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_6,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -7, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_7,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -8, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_8,
            MAX(CASE WHEN anomes = REPLACE(SUBSTRING(CAST(DATE_ADD('month', -9, CURRENT_DATE) AS VARCHAR), 1, 7), '-', '') THEN faturamento ELSE 0.0 END) AS mes_9
        FROM (
            SELECT
                numero_ponto_venda,
                SUBSTRING(CAST(anomesdia AS VARCHAR), 1, 6) AS anomes,
                SUM(valor_transacao) AS faturamento
            FROM {database}.{table}
            GROUP BY 
                numero_ponto_venda,
                SUBSTRING(CAST(anomesdia AS VARCHAR), 1, 6)
        )
        GROUP BY numero_ponto_venda
        """