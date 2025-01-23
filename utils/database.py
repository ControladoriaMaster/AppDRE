import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432")
        return conn 
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def load_movements():
    conn = get_connection()
    if conn is None:
        return {}

    queries = {
        "cidades": "SELECT * FROM protheus.dim_classe_valor;",
        "centro_custos": "SELECT * FROM protheus.dim_centro_custos;",
        "plano_contas": "SELECT * FROM protheus.dim_plano_contas;"
    }
    tables = {}
    try:
        cursor = conn.cursor()
        for query_name, query in queries.items():
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            tables[query_name] = pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"Erro ao carregar dados: {e}")
    finally:
        conn.close()
    return tables

def close_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        print(f"Erro ao fechar a conexão: {e}")

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
