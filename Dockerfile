FROM python:3.12

# Instalação do Poetry
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 -

# Adiciona Poetry ao PATH
ENV PATH="/root/.local/bin:$PATH"

# Copia os arquivos do projeto e instala as dependências
COPY . /src
WORKDIR /src
RUN poetry install

EXPOSE 8501

ENTRYPOINT ["poetry", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
