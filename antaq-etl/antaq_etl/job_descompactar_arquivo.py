# -*- coding: utf-8 -*-

import argparse
import logging
from io import BytesIO
import zipfile
import setup_env
from services.client_s3 import clientS3


def descompactarArquivo(bucket: str, chave: str):
    ano, dataset, etapa, nomeArquivo = chave.split("/")
    nomeArquivoTXT = nomeArquivo.replace('.zip', '.csv')
    chaveArquivoTXT = "/".join([ano, dataset, nomeArquivoTXT])

    objetoS3 = clientS3.get_object(Bucket=bucket, Key=chave).get("Body")
    buffer = BytesIO(objetoS3.read())
    zip = zipfile.ZipFile(buffer)

    for filename in zip.namelist():
        clientS3.upload_fileobj(
            zip.open(filename),
            BUCKET_STAGE,
            chaveArquivoTXT
        )

        logging.info(f"Arquivo {filename} salvo no bucket stage [{BUCKET_STAGE}/{chaveArquivoTXT}].")


def moverArquivoProcessado(chave: str):
    # Fazer cópia do arquivo para a pasta processado e apagar da pasta de download
    novaChave = chave.replace("downloaded", "processed")

    clientS3.copy_object(
        CopySource={'Bucket': BUCKET_RAW, 'Key': chave},
        Bucket=BUCKET_RAW,
        Key=novaChave)

    clientS3.delete_object(Bucket=BUCKET_RAW, Key=chave)

    logging.info(f"Arquivo {nomeArquivo} movido para a pasta de processados.")


parser = argparse.ArgumentParser(prog="Projeto FIEC ANTAQ - Descompactar arquivo *.zip",
                                 description="Job descompactar arquivos")

parser.add_argument("-a", "--ano")

try:
    args = parser.parse_args()
    anoArquivos = args.ano

    BUCKET_RAW = "antaq-raw"
    BUCKET_STAGE = "antaq-stage"

    objetosS3 = clientS3.list_objects(Bucket=BUCKET_RAW, Prefix=f"year={anoArquivos}")

    for obj in objetosS3.get("Contents"):
        chave = obj["Key"]
        ano, dataset, etapa, nomeArquivo = chave.split("/")

        if etapa == "downloaded":
            descompactarArquivo(BUCKET_RAW, chave)
            moverArquivoProcessado(chave)

except Exception as e:
    logging.error(f"Erro ao descompactar os arquivos do {anoArquivos}. [{e.args}]")
    raise e
