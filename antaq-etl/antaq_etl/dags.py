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
        nomeArquivo=f"{ano}{dataset['arquivo_compactado']}"
        links.append({
            "nome": dataset["nome"],
            "arquivo_compactado": nomeArquivo,
            "link":f"{URL_BASE}/{nomeArquivo}"
        })

    return links

listaAnos = obterListaAnosDisponiveis()

for ano in listaAnos:
    listaArquivos = obterLinksDownload(ano)
    for arquivo in listaArquivos:
        print(f"Download arquivo [{arquivo['nome']}] do ano {ano}. Link: {arquivo['link']}")

        with DAG(
                f"antaq_{ano}_{arquivo['nome']}_download_arquivo",
                description=f"Fazer o download arquivo [{arquivo['nome']}] do ano {ano}",
                start_date=datetime(2023, 1, 1),
                # schedule="@daily",
                tags=["ingest", "download"]
        ) as dag:
            task_fazer_download_arquivo = BashOperator(
                task_id=f"tsk_baixar_arquivo_zip_{arquivo['nome']}",
                bash_command=f"python {os.getcwd()}/dags/antaq-etl/antaq_etl/job_baixar_arquivo_zip.py -u {arquivo['link']}",
                dag=dag
            )



if __name__ == "__main__":
    listaAnos = obterListaAnosDisponiveis()

    for ano in listaAnos:
        listaArquivos = obterLinksDownload(ano)
        for arquivo in listaArquivos:
            print(f"Download arquivo [{arquivo['nome']}] do ano {ano}. Link: {arquivo['link']}")

            with DAG(
                    f"antaq_{arquivo['nome']}_download_arquivo",
                    description=f"Fazer o download arquivo [{arquivo['nome']}] do ano {ano}",
                    start_date=datetime(2023, 1, 1),
                    # schedule="@daily",
                    tags=["ingest", "download"]
            ) as dag:
                task_fazer_download_arquivo = BashOperator(
                    task_id=f"tsk_baixar_arquivo_zip_{arquivo['nome']}",
                    bash_command=f"python {os.getcwd()}/dags/job_baixar_arquivo_zip.py -u {arquivo['link']}",
                    dag=dag
                )
