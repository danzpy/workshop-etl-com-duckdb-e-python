import os
import gdown
import duckdb
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

def baixar_arquivos(URL: Path, DIRETORIO: Path) -> None:
    """
    Faz o download dos arquivos que estão presentes na URL informada. A pasta deverá ser pública para que
    o download ocorra.

    Args:
    
    URL = Caminho da pasta (Google Drive).
    DIRETORIO = Diretório onde serão armazenados os arquivos após o download.
    """
    os.makedirs(DIRETORIO_LOCAL, exist_ok=True)
    gdown.download_folder(URL_PASTA, output=DIRETORIO_LOCAL, quiet=False, use_cookies=False)

def listando_arquivos(DIRETORIO) -> list:
    """
    Verifica quais são os arquivos do tipo "csv" presentes no diretório especificado e registra seu
    diretório completo em uma lista.

    Args:

    DIRETORIO = Diretório onde ocorrerá a varredura.
    """
    arquivos = []
    for arquivo in os.listdir(DIRETORIO):
        if arquivo.endswith('.csv'):
            arquivos.append(f'{DIRETORIO}/{arquivo}')

    return arquivos

def ler_csv(CAMINHO: Path) -> duckdb.DuckDBPyRelation:
    """
    Cria e armazena um dataframe

    Args:
    CAMINHO = Caminho do arquivo que será lido.
    """
    df = duckdb.read_csv(CAMINHO)
    return df

ler_csv('./pasta_gdown/alunos_1.csv')


if __name__ == '__main__':
    URL_PASTA: Path = 'https://drive.google.com/drive/folders/1EZ6NMPSFaawuW6kzM_GBu-DHMx5JR_vw?usp=drive_link'
    DIRETORIO_LOCAL: Path = './pasta_gdown'

    #listando_arquivos(DIRETORIO_LOCAL)
    
    #baixar_arquivos(URL_PASTA, DIRETORIO_LOCAL)