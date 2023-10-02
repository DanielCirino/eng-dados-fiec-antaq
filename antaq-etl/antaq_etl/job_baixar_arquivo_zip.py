import argparse
import logging
import os
import requests
import setup_env
from services.client_s3 import clientS3

parser = argparse.ArgumentParser(prog="Projeto FIEC_ANTAQ - Download",
                                 description="Job Download Arquivo FIEC_ANTAQ")

parser.add_argument("-u", "--urls")

try:
    args = parser.parse_args()
    listaUrl = args.urls.split(",")

    for url in listaUrl:
        nomeArquivo = os.path.basename(url).lower()
        ano = nomeArquivo[:4]
        tipoArquivo=nomeArquivo[4:-4]
        diretorio = f"year={ano}/{tipoArquivo}/downloaded/{nomeArquivo}"

        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; "
                          "Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}

        response = session.get(url, headers=headers, verify=False)

        logging.info(f"Arquivo: {nomeArquivo} baixado com sucesso.")

        arquivoCompactado = response.content

        clientS3.put_object(
            Body=arquivoCompactado, Bucket="antaq-raw",
            Key=diretorio)

        logging.info(f"Arquivo salvo em: {diretorio}")


except Exception as e:
    logging.error(f"Erro ao baixar e salvar o arquivo {url}. [{e.args}]")
    raise e