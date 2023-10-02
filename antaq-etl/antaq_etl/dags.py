import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    listaAnos = [int(opcao.text) for opcao in opcoesAno]
    return listaAnos


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

    for ano in listaAnos:
        listaArquivos = obterLinksDownload(ano)
        urls = ",".join([arquivo["link"] for arquivo in listaArquivos])
        dataInicioDag = datetime.now()
        with DAG(
                f"antaq_{ano}_download_arquivos_zip",
                description=f"Fazer o download arquivos do ano {ano}",
                start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
                # schedule="@daily",
                tags=["ingest", "download", str(ano)]
        ) as dag:
            task_fazer_download_arquivo = BashOperator(
                task_id=f"tsk_baixar_arquivos_zip_{ano}",
                bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_baixar_arquivo_zip.py -u {urls}",
                dag=dag
            )


def criarDAGSDescompatarArquivo():
    listaAnos = obterListaAnosDisponiveis()
    for ano in listaAnos:
        dataInicioDag = datetime.now()
        with DAG(
                f"antaq_{ano}_descompactar_arquivos_zip",
                description=f"Descompactar arquivos do ano {ano}",
                start_date=datetime(dataInicioDag.year, dataInicioDag.month, dataInicioDag.day),
                # schedule="@daily",
                tags=["tranform", "unzip", str(ano)]
        ) as dag:
            task_descompactar_arquivo = BashOperator(
                task_id=f"tsk_descompactar_arquivos_{ano}",
                bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_descompactar_arquivo.py -a {ano}",
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
        task_descompactar_arquivo = BashOperator(
            task_id=f"tsk_salvar_dados_mssql_oltp",
            bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_salvar_dados_mssql.py",
            dag=dag
        )

criarDAGSDonwloadArquivoCompactado()
criarDAGSDescompatarArquivo()
criarDAGSalvarDadosOLTP()
