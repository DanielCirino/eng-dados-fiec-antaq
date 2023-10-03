# -*- coding: utf-8 -*-

import argparse
from pyspark.sql.functions import date_format, to_date, to_timestamp
import logging
import setup_env
from services import client_spark

spark = client_spark.obterSparkClient(f"ingest-dw")


def salvarDimensaoTempo():
    SQL_RANGE_DATAS = """
    SELECT 
        min(dt_atracacao) data_inicial,
        max(dt_atracacao) data_final
    FROM atracacao
    """
    dfRangeDatas = client_spark.getDataframeFromSql(spark, "antaq", SQL_RANGE_DATAS)
    dfDatas = dfRangeDatas.selectExpr("explode(sequence(data_inicial,data_final)) as data")
    dfDimTempo = dfDatas \
        .withColumn("ano", date_format(dfDatas.data, "yyyy")) \
        .withColumn("mes", date_format(dfDatas.data, "MM")) \
        .withColumn("dia", date_format(dfDatas.data, "dd")) \
        .withColumn("ano_mes", date_format(dfDatas.data, "yyyy-MM")) \
        .withColumn("trimestre", date_format(dfDatas.data, "yyyy-Q"))
    logging.info(f"Total registros de tempo: {dfDimTempo.count()}")
    print("===========================================")
    dfDimTempo.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão tempo no DW")
    client_spark.salvarDataFrameBancoDados(dfDimTempo, "antaq_dw", "dim_tempo")


def salvarDimensaoPortoInformante():
    SQL_DIM_PORTO = """
        SELECT
        	cod_porto_informante,cod_uf
        FROM atracacao 
        where 1=1
        group by cod_porto_informante, cod_uf   
    """
    dfDimPorto = client_spark.getDataframeFromSql(spark, "antaq", SQL_DIM_PORTO)
    logging.info(f"Total registros de porto: {dfDimPorto.count()}")
    print("===========================================")
    dfDimPorto.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão porto no DW")
    client_spark.salvarDataFrameBancoDados(dfDimPorto, "antaq_dw", "dim_porto")


def salvarDimensaoLocalizacao():
    SQL_DIM_LOCALIZACAO = """
        SELECT
        	municipio,cod_uf,nome_uf,regiao_hidrografica
        FROM antaq.dbo.atracacao 
        where 1=1
        group by nome_uf , cod_uf,municipio ,regiao_hidrografica  
    """
    dfDimLocalizacao = client_spark.getDataframeFromSql(spark, "antaq", SQL_DIM_LOCALIZACAO)
    logging.info(f"Total registros de proto: {dfDimLocalizacao.count()}")
    print("===========================================")
    dfDimLocalizacao.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão localização no DW")
    client_spark.salvarDataFrameBancoDados(dfDimLocalizacao, "antaq_dw", "dim_localizacao")


def salvarDimensaoBerco():
    SQL_DIM_BERCO = """
        SELECT
        	id_berco,nome_berco
        FROM antaq.dbo.atracacao 
        where 1=1
        group by id_berco ,nome_berco  
    """
    dfDimBerco = client_spark.getDataframeFromSql(spark, "antaq", SQL_DIM_BERCO)
    logging.info(f"Total registros de berco: {dfDimBerco.count()}")
    print("===========================================")
    dfDimBerco.show(5)
    print("===========================================")
    logging.info(f"Criando tabela dimensão porto no DW")
    client_spark.salvarDataFrameBancoDados(dfDimBerco, "antaq_dw", "dim_berco")


def salvarFatoAtracacao():
    SQL_FATO_ATRACACOES = """
        SELECT 
            id_atracacao, 
            cod_porto_informante, 
            nome_porto_informante, 
            alias_porto_informante,
            id_berco, 
            -- nome_berco, 
            complexo_portuario, 
            tipo_autoridade_portuaria, 
            dt_chegada, 
            dt_atracacao, 
            dt_inicio_operacao, 
            dt_fim_operacao, 
            dt_desatracacao, 
            CASE tipo_operacao
                WHEN '1' THEN 'Movimentacão de Carga'
                WHEN '2' THEN 'Passageiro'
                WHEN '3' THEN 'Apoio'
                WHEN '4' THEN 'Marinha'
                WHEN '5' THEN 'Abastecimento'
                WHEN '6' THEN 'Reparo/Manutenção'
                WHEN '7' THEN 'Misto'
                WHEN '8' THEN 'Retirada de Resíduos'
                ELSE tipo_operacao
            END tipo_operacao, 
            CASE tipo_navegacao 
                WHEN '1' THEN 'Navegação Interior'
                WHEN '2' THEN 'Apoio Portuário'
                WHEN '3' THEN 'Cabotagem'
                WHEN '4' THEN 'Apoio Marítimo'
                WHEN '5' THEN 'Longo Curso'
                ELSE tipo_navegacao
            END tipo_navegacao, 
            CASE nacionalidade_armador
                WHEN '1' THEN 'Brasileira'
                WHEN '2' THEN 'Estrangeira'
                ELSE nacionalidade_armador
            END nacionalidade_armador, 
            CASE contabiliza_movimentacao
                WHEN '1' THEN 'S'
                ELSE 'N'
            END contabiliza_movimentacao, 
            terminal, 
            municipio, 
            -- nome_uf, 
            -- cod_uf, 
            -- regiao_hidrografica, 
            -- cod_capitania, 
            cod_IMO, 
            tempo_espera_atracacao, 
            tempo_espera_operacao, 
            tempo_operacao, 
            tempo_espera_desatracacao, 
            tempo_atracado, 
            tempo_estadia
        FROM atracacao
    """
    dfFatoAtracacao = client_spark.getDataframeFromSql(spark, "antaq", SQL_FATO_ATRACACOES)
    logging.info(f"Total registros de atracacoes: {dfFatoAtracacao.count()}")
    print("===========================================")
    dfFatoAtracacao.show(5)
    print("===========================================")
    logging.info(f"Criando tabela fato atracacoes no DW")
    client_spark.salvarDataFrameBancoDados(dfFatoAtracacao, "antaq_dw", "atracacoes_fato")

parser = argparse.ArgumentParser(prog="Projeto Fiec Antaq - Carregar DW",
                                 description="Job inserir dados no datawarehouse")
def salvarFatoCarga():
    SQL_FATO_CARGA = """
        SELECT 
            id_carga, 
            c.id_atracacao, 
            cod_porto_origem, 
            cod_porto_destino, 
            cod_mercadoria, 
            c.tipo_operacao, 
            tipo_acondicionamento, 
            estado_container, 
            c.tipo_navegacao, 
            flag_autorizacao, 
            flag_cabotagem, 
            flag_cabotagem_movimentacao, 
            tamanho_container, 
            flag_longo_curso, 
            tipo_operacao_carga, 
            flag_offshore, 
            flag_via_interior, 
            percurso_vias_interiores, 
            percurso_interiores, 
            unica_natureza, 
            unico_capitulo, 
            unica_mercadoria, 
            natureza_carga, 
            sentido_operacao, 
            qtd_TEU_movimentacao, 
            qtd_unidades_movimentadas, 
            peso_bruto, 
            peso_liquido ,
            dt_atracacao,
            cod_porto_informante cod_porto_atracacao,
            municipio,
            YEAR(dt_atracacao) ano_atracacao, 
            MONTH(dt_atracacao) mes_atracacao,
            cod_uf_atracacao
        FROM carga c left join atracacao a on c.id_atracacao=a.id_atracacao
    """
    dfFatoCarga = client_spark.getDataframeFromSql(spark, "antaq", SQL_FATO_CARGA)
    logging.info(f"Total registros de atracacoes: {dfFatoCarga.count()}")
    print("===========================================")
    dfFatoCarga.show(5)
    print("===========================================")
    logging.info(f"Criando tabela fato atracacoes no DW")
    client_spark.salvarDataFrameBancoDados(dfFatoCarga, "antaq_dw", "cargas_fato")

try:
    args = parser.parse_args()
    logging.info(f"Lendo dados OLTP ")

    salvarDimensaoBerco()
    salvarDimensaoLocalizacao()
    salvarDimensaoTempo()
    salvarDimensaoPortoInformante()
    salvarFatoAtracacao()
    salvarFatoCarga()

except Exception as e:
    logging.error(f"Erro ao salvar dados no DW. [{e.args}]")
    raise e
