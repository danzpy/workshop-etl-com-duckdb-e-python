import os
import gdown
import duckdb
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

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

def transformar(df: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    """
    Recebe um duckdb.DuckDBPyRelation e retorna um pd.DataFrame com uma coluna "Status", de acordo
    com a nota do Aluno.

    Args:
    df = duckdb.DuckDBPyRelatio
    """

    df_tratado = duckdb.sql("SELECT *, CASE WHEN NOTA > 60 THEN 'APROVADO' ELSE 'REPROVADO' END AS status FROM df").df()

    return df_tratado

def carregar_postgres(df: pd.DataFrame, tabela: str) -> None:
    """
    Recebe um pd.DataFrame e armazena seus dados na tabela informada.

    Args:
    df = pd.Dataframe
    tabela = Tabela destino (postgres).
    """
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    df.to_sql(tabela, con=engine, if_exists='append', index=False)
    
    print(f'Dados inseridos na {tabela} com sucesso.')

if __name__ == '__main__':
    URL_PASTA: Path = os.getenv("URL_PASTA")
    DIRETORIO_LOCAL: Path = './pasta_gdown'
    #baixar_arquivos(URL_PASTA, DIRETORIO_LOCAL)
    lista_arquivos = listando_arquivos(DIRETORIO_LOCAL)
    for arquivo in lista_arquivos:
        duckdb_df = ler_csv(arquivo)
        pandas_df = transformar(duckdb_df)
        carregar_postgres(pandas_df, 'status_alunos')

    
    