import streamlit as st
import pandas as pd
from utils import get_connection  # Importe a função de conexão


# Função para carregar dados de múltiplas tabelas usando várias consultas
def load_movements():
    conn = get_connection()  # Obtém a conexão do utils.py
    if conn is None:
        st.error("Não foi possível conectar ao banco de dados.")
        return {}  # Retorna um dicionário vazio

    # Dicionário de consultas
    queries = {
        "cidades": "SELECT * FROM protheus.dim_classe_valor;",
        "centro_custos": "SELECT * FROM protheus.dim_centro_custos;",
        "plano_contas": "SELECT * FROM protheus.dim_plano_contas;"
    }
    
    tables = {}  # Dicionário para armazenar os DataFrames
    try:
        cursor = conn.cursor()
        for query_name, query in queries.items():
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            tables[query_name] = pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Erro ao executar consultas: {e}")
    finally:
        conn.close()  # Fecha a conexão ao finalizar as consultas

    return tables

st.set_page_config(page_title="Tratamento de Dados para DRE")

st.write("### Tratamento de Dados para DRE - Informações Gerais")

st.markdown(
    """
    Bem-vindo ao nosso aplicativo especializado em tratamento e transformação de dados para o banco de dados da DRE. 
    Reconhecemos a importância de processar informações de forma eficiente e segura, e por isso, 
    desenvolvemos uma plataforma segmentada em módulos que representam diferentes etapas desse tratamento.

  ### Movimentação de Estoque:
    - [Clique aqui](https://drive.google.com/uc?export=download&id=1MvaaoHyCcB85T6LuO1EWmGl259G6P2Qz) para baixar a planilha com
    modelo dos dados de entrada para esse processo.

  """
  """
  ### Conteúdo de Programação:
    - [Clique aqui](https://drive.google.com/uc?export=download&id=1-SvAZs0izjIHUarTWf0bLhPnE6X9KJ6c) para baixar a planilha com
    modelo dos dados de entrada para esse processo.
  
  """
  """
  ### Acompanhamento de Terceirizadas:
    - [Clique aqui](https://drive.google.com/uc?export=download&id=1YRy80_Jdft3rIht9nWNZhUACmD1ZvwAH) para baixar a planilha com
    modelo dos dados de entrada para esse processo.
  
  """
  """
  ### Faturamento e Impostos:
    - [Clique aqui](https://drive.google.com/uc?export=download&id=1G6r7551I3NDW68m2apCBWeAUuMeLdDr1) para baixar a planilha com
    modelo dos dados de entrada para esse processo.
  
  """
  """
  ### Despesas Contábeis:
    - [Clique aqui](https://drive.google.com/uc?export=download&id=1mk_Co3eCiXQjMLVR9LvswbpsGfNSob_U) para baixar a planilha com
    modelo dos dados de entrada para esse processo.
  
  """
  """
  ### Pagamentos:
    - [Clique aqui](https://drive.google.com/uc?export=download&id=1bjXMhg_jJ4t5d0FPkG0sF_2HofDtLw2p) para baixar a planilha com
    modelo dos dados de entrada para esse processo.
    
  """ 
  """
  ### União dos Arquivos da DRE:
    Esse processo faz a junção de todos os arquivos gerados dos processos  anteriores em apenas uma arquivo. Para isto, bata apenas
    carregar os arquivos dos processos e depois clicar no botão iniciar. Depois de algum tempo será gerado um arquivo final com todos 
    os arquivos unidos.

  """ 
)
