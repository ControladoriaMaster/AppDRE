import psycopg2
import pandas as pd
import io
from dotenv import load_dotenv
import os
import streamlit as st
from datetime import datetime

load_dotenv()

@st.cache_resource
def get_connection():
    """
    Retorna uma conexão única ao banco de dados.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_DATABASE"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
           )
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def close_connection(conn):
    """
    Fecha a conexão ao banco de dados.
    """
    if conn:
        conn.close()
# Função para preencher o mês
def preencher_data_por_mes(entrada, nome_coluna_mes, nome_coluna_data, ano):
    meses = {
        'Janeiro': '01-15',
        'Fevereiro': '02-15',
        'Março': '03-15',
        'Abril': '04-15',
        'Maio': '05-15',
        'Junho': '06-15',
        'Julho': '07-15',
        'Agosto': '08-15',
        'Setembro': '09-15',
        'Outubro': '10-15',
        'Novembro': '11-15',
        'Dezembro': '12-15'
    }
    
    # Valida o nome do mês
    if entrada[nome_coluna_mes].iloc[0] not in meses:
        raise ValueError(f"Mês inválido: {entrada[nome_coluna_mes].iloc[0]}")
    
    # Define a data correspondente
    entrada[nome_coluna_data] = entrada[nome_coluna_mes].apply(
        lambda mes: datetime.strptime(f"{ano}-{meses[mes]}", "%Y-%m-%d")
    )

    # Data
    data_string = f"{ano}-{meses[nome_coluna_mes]}"
    data = datetime.strptime(data_string, "%Y-%m-%d")

    # Preencher a coluna do DataFrame com a data
    entrada[nome_coluna_data] = data 

# Função para carregar os dados
def load_data(conn, queries):
    """
    Executa várias consultas SQL em um banco de dados e retorna os resultados como DataFrames.

    Args:
        conn: conexão com o banco de dados.
        queries: dicionário com nome da tabela como chave e consulta SQL como valor.

    Returns:
        dict: dicionário com nomes das tabelas como chaves e DataFrames como valores.
    """
    tables = {}
    try:
        cursor = conn.cursor()
        for table_name, query in queries.items():
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            tables[table_name] = pd.DataFrame(data, columns=columns)
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar dados: {e}")
    return tables

# Função para processar os arquivos em Excel
def processar_excel(uploaded_file):
    """
    Processa um arquivo Excel carregado pelo usuário.

    Args:
        uploaded_file: arquivo Excel carregado.

    Returns:
        DataFrame: DataFrame carregado do Excel.
    """
    try:
        # Carrega o arquivo Excel em um DataFrame
        df = pd.read_excel(uploaded_file)
        return df
    except Exception as e:
        raise RuntimeError(f"Erro ao processar arquivo Excel: {e}")

#Função para salvar o arquivo em Excel
def salvar_em_excel(df, nome_arquivo):
    """
    Salva um DataFrame em um arquivo Excel na memória.

    Args:
        df: DataFrame a ser salvo.
        nome_arquivo: nome do arquivo.

    Returns:
        BytesIO: arquivo Excel salvo na memória.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

# Construção do dicionário para colocar o nome dos produtos padronizados na coluna de detalhamento
DETALHAMENTO_PRODUTOS = {
            'CABO DE REDE UTP CAT 5 BRANCO': 'CABO DE REDE',
            'CABO DROP (MONOFIBRA) FLAT LOW': 'CABO DROP',
            'CABO OPTICO CONECTORIZADO DROP COMPACTO LOW FRICITION BLI-CM-01-AR-LSZH': 'CABO OPTICO CONECTORIZADO',
            'CONECTOR PRE POLIDO CLICK RAPIDO SC/APC': 'CONECTOR PRE POLIDO',
            'CONECTOR PRE POLIDO SC/APC ROSQ TIPO B': 'CONECTOR PRE POLIDO',
            'CONECTOR PRE POLIDO SC/UPC': 'CONECTOR PRE POLIDO',
            'ONT GPON G-1425G-A NOKIA': 'ONT GPON',
            'ONT GPON NOVA PHYHOME AC1200': 'ONT GPON',
            'ONT WIFI NOVA INTELBRAS': 'ONT WIFI',
            'ONT ZTE NOVA GPON WIFI AC1200 MBPS': 'ONT GPON',
            'ONU EPON NOVA': 'ONU EPON NOVA',
            'ONU GEPON': 'ONU GEPON',
            'ONU GPON': 'ONU GPON',
            'ONU XPON HIBRIDA': 'ONU XPON',
            'ROTEADOR MERCUSYS N MW301R 2 ANTENAS – ATÉ 50 MBPS': 'ROTEADOR MERCUSYS 2 ANTENAS',
            'ROTEADOR MULT. NOVO ZTE 4 ANT. BRANCO': 'ROTEADOR MULTILASER 4 ANTENAS NOVO',
            'ROTEADOR MULTILASER NOVO 2 ANTENAS – ATÉ 50 MBPS': 'ROTEADOR MULTILASER 2 ANTENAS NOVO',
            'SIMCARD (MVNO)': 'SIMCARD (MVNO)',
            'SUPORTE CAVALETE': 'SUPORTE CAVALETE',
            'SUPORTE TELHA 3/4': 'SUPORTE TELHA',
            'TUBO 3 METROS P/ NET WIRELLES': 'TUBO 3 METROS'
            }