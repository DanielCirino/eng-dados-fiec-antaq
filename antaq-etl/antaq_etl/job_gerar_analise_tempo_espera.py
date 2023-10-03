# -*- coding: utf-8 -*-

import argparse
import logging
from datetime import datetime
import setup_env
from services import client_spark

BUCKET_ANALYTICS = "antaq-analytics"
spark = client_spark.obterSparkClient(f"analytics")


def salvarAnaliseTempoEspera():
    SQL_ANALISE = """
        SELECT 
            cod_uf,
            regiao_hidrografica,
            ano ano_atual, 
            mes mes_atual, 
            qtd_atracacoes, 
            tempo_espera_atracacao,
            tempo_atracado,
             LAG(ano,12) OVER ( ORDER BY ano, mes) AS ano_anterior, 
             LAG(mes,12) OVER ( ORDER BY ano, mes) AS mes_comparacao,
             LAG(qtd_atracacoes,12) OVER ( ORDER BY ano, mes) AS qtd_atracacoes_periodo_anterior,
             qtd_atracacoes - LAG(qtd_atracacoes,12) OVER (ORDER BY ano, mes) AS diferenca,
             (qtd_atracacoes - LAG(qtd_atracacoes,12) OVER (ORDER BY ano, mes)) / CAST(qtd_atracacoes AS DECIMAL) AS variacao_percentual
        FROM (
            SELECT
                dt.ano,
                dt.mes,
                dl.cod_uf ,
                dl.regiao_hidrografica,
                count(*) qtd_atracacoes, 
                sum(af.tempo_espera_atracacao) tempo_espera_atracacao,
                sum(af.tempo_atracado) tempo_atracado
            FROM atracacoes_fato  af 
                left join dim_tempo dt on CAST(af.dt_atracacao as date) = CAST (dt.[data] as date )
                left join dim_localizacao dl on dl.municipio =af.municipio 
                left join dim_porto dp on dp.cod_porto_informante = af.cod_porto_informante 
            WHERE dt.ano in ('2023','2022','2021')
                GROUP BY
                dt.ano, dt.mes,
                dl.cod_uf ,
                dl.regiao_hidrografica) as atracacoes_mes
        WHERE 1=1 
            -- and cod_uf='CE' 
    """
    dfAnalise = client_spark.getDataframeFromSql(spark, "antaq_dw", SQL_ANALISE)
    logging.info(f"Total registros da analise: {dfAnalise.count()}")
    print("===========================================")
    dfAnalise.show(5)
    print("===========================================")
    logging.info(f"Salvando análise no datalake")
    agora = datetime.now()
    chave = f"tempo_espera/year={agora.year}/month={agora.month}/day={agora.day}"

    dfAnalise.write.parquet(
        path=f"s3a://{BUCKET_ANALYTICS}/{chave}",
        mode="overwrite")

    logging.info(f"Análise tempo de espera salvo no bucket analytics [{chave}].")


parser = argparse.ArgumentParser(prog="Projeto Fiec Antaq - Análise tempo de espera",
                                 description="Job gerar análise tempo espera")
try:
    args = parser.parse_args()
    logging.info(f"Lendo dados do datawarehouse ")

    salvarAnaliseTempoEspera()

except Exception as e:
    logging.error(f"Erro ao salvar análise tempo de espera. [{e.args}]")
    raise e
