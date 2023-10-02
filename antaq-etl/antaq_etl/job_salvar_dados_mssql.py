# -*- coding: utf-8 -*-

import argparse
import logging

from pyspark.sql.functions import col, to_timestamp
import setup_env
from services.client_s3 import clientS3
from services import client_spark

BUCKET_STAGE = "antaq-stage"

def processarArquivoAtracacao(spark, dfArquivo, modoEscrita="overwrite"):
    dfArquivo.createOrReplaceTempView("tbl_atracacao")

    sqlQuery = """
        select 
            `IDAtracacao` id_atracacao,
            `CDTUP` cod_porto_informante,
            `IDBerco` id_berco,
            `Berço` nome_berco,
            `Porto Atracação` nome_porto_informante,
            `Apelido Instalação Portuária` alias_porto_informante,
            `Complexo Portuário` complexo_portuario,
            `Tipo da Autoridade Portuária` tipo_autoridade_portuaria,
            `Data Atracação` dt_atracacao,
            `Data Chegada` dt_chegada,
            `Data Desatracação` dt_desatracacao,
            `Data Início Operação` dt_inicio_operacao,
            `Data Término Operação` dt_fim_operacacao,
            `Tipo de Operação` tipo_operacao,
            `Tipo de Navegação da Atracação` tipo_navegacao,
            `Nacionalidade do Armador` nacionalidade_armador,
            `FlagMCOperacaoAtracacao` contabiliza_movimentacao,
            `Terminal` terminal,
            `Município` municipio,
            `UF` nome_uf,
            `SGUF` cod_uf,
            `Região Geográfica` regiao_hidrografica,
            `Nº da Capitania` cod_capitania,
            `Nº do IMO` cod_IMO
        from tbl_atracacao
    """

    df = spark.sql(sqlQuery) \
        .withColumn("dt_atracacao", to_timestamp("dt_atracacao", "dd/MM/yyyy HH:mm:ss")) \
        .withColumn("dt_desatracacao", to_timestamp("dt_desatracacao", "dd/MM/yyyy HH:mm:ss")) \
        .withColumn("dt_chegada", to_timestamp("dt_chegada", "dd/MM/yyyy hh:mm:ss")) \
        .withColumn("dt_inicio_operacao", to_timestamp("dt_inicio_operacao", "dd/MM/yyyy HH:mm:ss")) \
        .withColumn("dt_fim_operacacao", to_timestamp("dt_fim_operacacao", "dd/MM/yyyy HH:mm:ss")) \

    print(f"Total registros no arquivo: {df.count()}")

    print("===========================================")
    # df.printSchema()
    print("===========================================")

    print(f"Salvando dados de ATRACAÇÃO no banco de dados")
    client_spark.salvarDataFrameBancoDados(df, "antaq", "atracacao", modoEscrita)

def processarArquivoCarga(spark, dfArquivo, modoEscrita="overwrite"):
    dfArquivo.createOrReplaceTempView("tbl_carga")
    sqlQuery = """
        select 
            `IDCarga` id_carga,
            `IDAtracacao` id_atracacao,
            `Origem` cod_porto_origem,
            `Destino` cod_porto_destino,
            `CDMercadoria` cod_mercadoria,
            `Tipo Operação da Carga` tipo_operacao,
            `Carga Geral Acondicionamento` tipo_acondicionamento,
            `ConteinerEstado` estado_container,
            `Tipo Navegação` tipo_navegacao,
            `FlagAutorizacao` flag_autorizacao,
            `FlagCabotagem` flag_cabotagem,
            `FlagCabotagemMovimentacao` flag_cabotagem_movimentacao,
            `FlagConteinerTamanho` tamanho_container,
            `FlagLongoCurso` flag_longo_curso,
            `FlagMCOperacaoCarga` tipo_operacao_carga,
            `FlagOffshore` flag_offshore,
            `FlagTransporteViaInterioir` flag_via_interior,
            `Percurso Transporte em vias Interiores` percurso_vias_interiores,
            `Percurso Transporte Interiores` percurso_interiores,
            `STNaturezaCarga` unica_natureza,
            `STSH2` unico_capitulo,
            `STSH4` unica_mercadoria,
            `Natureza da Carga` natureza_carga,
            `Sentido` sentido_operacao,
            CAST(`TEU` AS INT) qtd_TEU_movimentacao,
            CAST(`QTCarga` AS INT) qtd_unidades_movimentadas,
            CAST(REPLACE(`VLPesoCargaBruta`,',','.') AS DOUBLE) peso_bruto,
            CASE 
                WHEN `Carga Geral Acondicionamento` <> 'Conteinerizada'  THEN CAST(REPLACE(`VLPesoCargaBruta`,',','.') AS DOUBLE)
                WHEN `ConteinerEstado` = 'Vazio' THEN 0
                WHEN `FlagConteinerTamanho` = '20' THEN CAST(REPLACE(`VLPesoCargaBruta`,',','.') AS DOUBLE) - 2.12
                WHEN `FlagConteinerTamanho` = '40' THEN CAST(REPLACE(`VLPesoCargaBruta`,',','.') AS DOUBLE) - 4
                ELSE CAST(REPLACE(`VLPesoCargaBruta`,',','.') AS DOUBLE)
            END peso_liquido
        from tbl_carga
    """

    df = spark.sql(sqlQuery)

    print(f"Total registros no arquivo: {df.count()}")

    print("===========================================")
    # df.printSchema()
    print("===========================================")

    print(f"Salvando dados de CARGA no banco de dados")
    client_spark.salvarDataFrameBancoDados(df, "antaq", "carga", modoEscrita)

parser = argparse.ArgumentParser(prog="Projeto Fiec Antaq - Salvar dados OLTP",
                                 description="Job inserir dados no MSSQL ")

parser.add_argument("-a", "--ano")

try:
    args = parser.parse_args()
    anoArquivos = args.ano

    objetosS3 = clientS3.list_objects(Bucket=BUCKET_STAGE)
    spark = client_spark.obterSparkClient(f"ingest-oltp")

    contArquivosAtracacao = 0
    contArquivosCarga = 0

    for obj in objetosS3.get("Contents"):
        chave = obj["Key"]
        ano, dataset, nomeArquivo = chave.split("/")
        dfArquivo = spark \
            .read \
            .csv(f"s3a://{BUCKET_STAGE}/{chave}", header=True)

        if dataset == "atracacao":
            modoEscrita = "overwrite" if contArquivosAtracacao == 0 else "append"
            processarArquivoAtracacao(spark, dfArquivo, modoEscrita)
            contArquivosAtracacao += 1

        if dataset == "carga":
            modoEscrita = "overwrite" if contArquivosCarga == 0 else "append"
            processarArquivoCarga(spark, dfArquivo, modoEscrita)
            contArquivosCarga += 1
    spark.stop()

except Exception as e:
    logging.error(f"Erro ao salvar dados no MS SQL Server. [{e.args}]")
    raise e
