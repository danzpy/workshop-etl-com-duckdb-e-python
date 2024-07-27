import os
import gdown
import duckdb
import pandas as pd
import pipeline as pp
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
from duckdb import DuckDBPyRelation
from pandas import DataFrame

from datetime import datetime

load_dotenv()

def conectar_banco() -> None:
    """
    Conecta ao banco de dados DuckDB; cria o banco se não existir.
    """
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con: duckdb.DuckDBPyConnection) -> None:
    """
    Cria a tabela historico_arquivos, caso não exista.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos(
                arquivo VARCHAR(30),
                dt_processamento TIMESTAMP
                )
    """)


def registrar_arquivo(con: duckdb.DuckDBPyConnection, arquivo: str) -> None:
    """
    Insere os arquivos que serão processados na tabela historico_arquivos
    """
    con.execute("""
        INSERT INTO historico_arquivos (arquivo, dt_processamento)
        VALUES (?, ?)
    """, (arquivo, datetime.now()))

def ler_arquivos(arquivo: Path) -> pd.DataFrame:
    """
    Verifica a extensão do arquivo presente no caminho informado e retorna um pd.DataFrame.

    Args:
    CAMINHO: Caminho/arquivo que originará o DataFrame.
    """
    
    if arquivo.endswith('.csv'):
        df = pd.read_csv(arquivo)
    elif arquivo.endswith('.parquet'):
        df = pd.read_parquet(arquivo)
    elif arquivo.endswith('.json'):
        df = pd.read_json(arquivo, orient='records', lines=True)

    return df

def pipeline() -> list:
    """
    Executa toda a ETL do projeto.
    """
    logs = []

    URL_PASTA: Path = os.getenv("URL_PASTA")
    DIRETORIO_LOCAL: Path = './pasta_gdown'
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    try:
        logs.append(f"Baixando arquivos do Google Drive para o diretório {DIRETORIO_LOCAL}.")
        pp.baixar_arquivos(URL_PASTA, DIRETORIO_LOCAL)
    except Exception as e:
        logs.append(f'Não foi possível se conectar ao GoogleDrive: {str(e)}')
        return logs

    con = conectar_banco()
    inicializar_tabela(con)
    
    logs.append(f"Verificando arquivos que foram baixados no diretório {DIRETORIO_LOCAL}.")
    lista_arquivos = pp.listando_arquivos(DIRETORIO_LOCAL)
    
    for arquivo in lista_arquivos:
        nome_arquivo = os.path.basename(arquivo)
        logs.append(f"Processando arquivo: {nome_arquivo}")
        
        check = con.sql(f"SELECT arquivo FROM historico_arquivos WHERE arquivo = '{nome_arquivo}'").df()
        if check.empty:          
            duckdb_df = ler_arquivos(arquivo)
            pandas_df = pp.transformar(duckdb_df)
            
            logs.append(f"Inserindo arquivo {nome_arquivo} no Postgres: ")
            pp.carregar_postgres(pandas_df, 'status_alunos')
            
            registrar_arquivo(con, nome_arquivo)
        else:
            log_msg = f'Arquivo {nome_arquivo} já foi processado anteriormente.'
            logs.append(log_msg)
            print(log_msg)
    
    logs.append("Processo de ETL completo.")
    return logs

if __name__ == '__main__':
    pipeline()

