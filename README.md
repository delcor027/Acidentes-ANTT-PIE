# Projeto de Extração e Processamento de Dados da PRF

Este projeto tem como objetivo extrair, processar e carregar dados abertos da Polícia Rodoviária Federal (PRF) em diferentes camadas de um banco de dados SQL Server. O pipeline é dividido em quatro etapas principais:

1. Extração e organização dos dados.
2. Processamento e carregamento dos dados na camada Bronze.
3. Processamento e carregamento dos dados na camada Silver.
4. Cópia dos dados da camada Silver para a camada Gold.

## Estrutura do Projeto

- **DataExtract.py**: Responsável por baixar e extrair arquivos CSV de dados abertos da PRF.
- **LayerBronze.py**: Processa e carrega os dados dos arquivos CSV para a camada Bronze do banco de dados.
- **LayerSilver.py**: Processa e transforma os dados da camada Bronze para a camada Silver.
- **LayerGold.py**: Copia os dados transformados da camada Silver para a camada Gold.
- **Main.py**: Orquestra a execução das diferentes etapas do pipeline.

## Pré-requisitos

- Python 3.7+
- SQL Server
- [Poetry](https://python-poetry.org/)

## Instalação

1. Clone o repositório:

    ```bash
    git clone https://github.com/seu-usuario/seu-repositorio.git
    cd seu-repositorio
    ```

2. Instale as dependências utilizando o Poetry:

    ```bash
    poetry install
    ```

## Configuração do Banco de Dados

Antes de executar o script, certifique-se de que o SQL Server está configurado corretamente e que a string de conexão no arquivo `Main.py` está apontando para o seu banco de dados.

## Execução

Para executar o pipeline de extração e processamento de dados, utilize o comando:

```bash
poetry run python Main.py
```

## Descrição dos Arquivos

**`DataExtract.py`**

Este módulo contém classes e métodos para:

- Configurar o logger.
- Extrair links de download da página de dados abertos da PRF.
- Baixar e extrair arquivos CSV.
- Renomear e organizar arquivos extraídos.

**`LayerBronze.py`**

Este módulo contém classes e métodos para:

- Processar e carregar dados de arquivos CSV para a camada Bronze do banco de dados.
- Ler arquivos CSV e inserir dados nas tabelas correspondentes no banco de dados.

**`LayerSilver.py`**

Este módulo contém classes e métodos para:

- Limpar e transformar dados da camada Bronze.
- Carregar dados transformados para a camada Silver do banco de dados.

**`LayerGold.py`**

Este módulo contém classes e métodos para:

- Copiar dados transformados da camada Silver para a camada Gold do banco de dados.

**`Main.py`**

Este módulo orquestra a execução de todas as etapas do pipeline:

- Extrai e organiza os dados a partir de um URL.
- Processa e carrega os dados na camada Bronze.
- Processa e carrega os dados na camada Silver.
- Copia os dados da camada Silver para a camada Gold.
