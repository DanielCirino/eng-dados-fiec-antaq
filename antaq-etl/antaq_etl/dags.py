import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from bs4 import BeautifulSoup as bs4

session = requests.Session()
URL_BASE = "https://web3.antaq.gov.br/ea/txt"

datasets = [
    {
        "nome": "Atracação",
        "arquivo_compactado": "Atracacao.zip",
    }, {
        "nome": "Carga",
        "arquivo_compactado": "Carga.zip",
    }
]


def fazerRequisicao(url: str):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}
        respostaHttp = session.get(url, headers=headers)
        assert respostaHttp.status_code == 200

        conteudoHtml = respostaHttp.content
        docHtml = bs4(conteudoHtml, 'html.parser')
        return docHtml
    except Exception as e:
        print(e)
        raise e


def obterListaAnosDisponiveis():
    docHtml = fazerRequisicao("https://web3.antaq.gov.br/ea/sense/download.html#pt")

    opcoesAno = docHtml.select("#anotxt option")
    listaAnos = [opcao.text for opcao in opcoesAno]
    return listaAnos[:3]


def obterLinksDownload(ano: int):
    links = []

    for dataset in datasets:
        nomeArquivo = f"{ano}{dataset['arquivo_compactado']}"
        links.append({
            "nome": dataset["nome"],
            "arquivo_compactado": nomeArquivo,
            "link": f"{URL_BASE}/{nomeArquivo}"
        })

    return links


def criarDAGSDonwloadArquivoCompactado():
    listaAnos = obterListaAnosDisponiveis()
    listaArquivos = []
    for ano in listaAnos:
        listaArquivos.extend(obterLinksDownload(ano))

    urls = ",".join([arquivo["link"] for arquivo in listaArquivos])
    dataInicioDag = datetime.now()

    with DAG(
            f"antaq_download_arquivos_zip",
            description=f"Fazer o download de {len(listaArquivos)} arquivos de {len(listaAnos)} anos",
            start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
            # schedule="@daily",
            tags=["ingest", "download", str(ano)]
    ) as dag:
        return BashOperator(
            task_id=f"tsk_baixar_arquivos_zip",
            bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_baixar_arquivo_zip.py -u {urls}",
            dag=dag
        )


def criarDAGSDescompatarArquivo():
    listaAnos = obterListaAnosDisponiveis()

    anosExtracao = ",".join(listaAnos)
    dataInicioDag = datetime.now()

    with DAG(
            f"antaq_descompactar_arquivos_zip",
            description=f"Descompactar arquivos de  {len(listaAnos)} anos.",
            start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
            # schedule="@daily",
            tags=["tranform", "unzip"]
    ) as dag:
         return BashOperator(
            task_id=f"tsk_descompactar_arquivos",
            bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_descompactar_arquivo.py -a {anosExtracao}",
            dag=dag
        )


def criarDAGSalvarDadosOLTP():
    dataInicioDag = datetime.now()
    with DAG(
            f"antaq_salvar_dados_mssql_oltp",
            description=f"Salvar dados do datalake na base de dados SQL Server",
            start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
            # schedule="@daily",
            tags=["ingest", "oltp"]
    ) as dag:
        return BashOperator(
            task_id=f"tsk_salvar_dados_mssql_oltp",
            bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_salvar_dados_mssql.py",
            dag=dag
        )


def criarDAGSalvarDadosDW():
    dataInicioDag = datetime.now()
    with DAG(
            f"antaq_salvar_dados_mssql_dw",
            description=f"Salvar dados da base OLTP na base de dados do Datawarehouse",
            start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
            # schedule="@daily",
            tags=["ingest", "datawarehouse"]
    ) as dag:
        return BashOperator(
            task_id=f"tsk_salvar_dados_mssql_dw",
            bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_salvar_dados_datawarehouse.py",
            dag=dag
        )

def criarDAGGerarAnaliseTempoEspera():
    dataInicioDag = datetime.now()
    with DAG(
            f"antaq_gerar_analise_tempo_espera",
            description=f"Salvar dados da base OLTP na base de dados do Datawarehouse",
            start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
            # schedule="@daily",
            tags=["analytics"]
    ) as dag:
        return BashOperator(
            task_id=f"tsk_gerar_analise_tempo_espera",
            bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_gerar_analise_tempo_espera.py",
            dag=dag
        )

task_fazer_download_arquivo = criarDAGSDonwloadArquivoCompactado()
task_descompactar_arquivo = criarDAGSDescompatarArquivo()
task_salvar_dados_oltp = criarDAGSalvarDadosOLTP()
task_salvar_dados_mssql_dw = criarDAGSalvarDadosDW()
task_gerar_analise_tempo_espera = criarDAGGerarAnaliseTempoEspera()

trigger_descompactar_arquivos= TriggerDagRunOperator(
    task_id=f"tgr_descompactar_arquivos",
    trigger_dag_id=f"antaq_descompactar_arquivos_zip",
)

trigger_salvar_banco_dados = TriggerDagRunOperator(
    task_id=f"tgr_salvar_dados_OLTP",
    trigger_dag_id=f"antaq_salvar_dados_mssql_oltp",
)

trigger_salvar_dados_DW = TriggerDagRunOperator(
    task_id=f"tgr_salvar_dados_DW",
    trigger_dag_id=f"antaq_salvar_dados_mssql_dw",
)

trigger_gerar_analise_tempo_espera = TriggerDagRunOperator(
    task_id=f"tgr_analise_tempo_espera",
    trigger_dag_id=f"antaq_gerar_analise_tempo_espera",
)

task_fazer_download_arquivo >> trigger_descompactar_arquivos
task_descompactar_arquivo >> trigger_salvar_banco_dados
task_salvar_dados_oltp >> trigger_salvar_dados_DW
task_salvar_dados_mssql_dw >> trigger_gerar_analise_tempo_espera

