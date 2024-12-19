import streamlit as st
import pandas as pd
import numpy as np
from utils import (get_connection, close_connection, load_data, salvar_em_excel, processar_excel, DETALHAMENTO_PRODUTOS)

st.write('## Processo de Movimentação de Estoque')

with st.container(): # Lista de seleção dos meses e informação do ano

    st.markdown(
        """
        #### Atenção!
        **É necessário o preenchimento dos campos mês e ano.** Sem o preenchimento correto do **mês e do ano**, *o processamento dos 
        arquivos não será realizado.* Estes campos são cruciais para garantir a precisão e a contextualização dos dados.
    """
    )
    
    # Dividindo essa parte da página em duas
    coluna1, coluna2 = st.columns(2)

    with coluna1:
        st.write("Selecione o mês do relatório")
        # Criando a lista de meses e o seletor 
        meses = [" ", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
        mes = st.selectbox("Mês", meses)
        st.write("O mês selecionado é: ", mes)
    with coluna2:
        st.write("Digite o ano do relatório")
        ano = st.text_input("Ano")
        st.write ("O ano informado é: ", ano)

with st.container(): # Consultas ao banco de dados
    
    def carregar_dados():
        queries = {
            "cidades": "SELECT * FROM protheus.dim_classe_valor;",
            "centro_custos": "SELECT * FROM protheus.dim_centro_custos;",
            "plano_contas": "SELECT * FROM protheus.dim_plano_contas;"
        }

        conn = get_connection()
        if not conn:
            st.error("Não foi possível conectar ao banco de dados.")
            return {}

        try:
            tables = {}
            cursor = conn.cursor()
            for key, query in queries.items():
                cursor.execute(query)
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                tables[key] = pd.DataFrame(data, columns=columns)
        except Exception as e:
            st.error(f"Erro ao carregar os dados: {e}")
            return {}
        finally:
            close_connection(conn)

        return tables

    if "loaded_data" not in st.session_state:
        st.session_state.loaded_data = carregar_dados()

    cidades = st.session_state.loaded_data.get("cidades", pd.DataFrame())
    centro_custos = st.session_state.loaded_data.get("centro_custos", pd.DataFrame())
    plano_contas = st.session_state.loaded_data.get("plano_contas", pd.DataFrame())

with st.container(): # Processo de Movimentação de Estoque
# Processo de Movimentação de Estoque
    def processar_movimentacao(uploaded_file):
        
        # Carregue o arquivo Excel para um DataFrame
        entrada = processar_excel(uploaded_file)
        
        # Excluindo os dados onde a coluna B1_XCTB possui valor N
        entrada = entrada[entrada['B1_XCTB'] == 'S']
        
        # Criando o DataFrame de tratamento aproveitando algumas colunas dos dados de entrada
        tratamento = pd.DataFrame(entrada, columns = ['COD','PRODUTO','QTDE','CUSTO_MEDIO', 'MED_NF_ENT', 'DATA_MOV','OBS_ID_OS'])
        
        # Criando outro dataframe com os mesmo dados anteriores mas adicionando as colunas que serão calculadas
        tratamento = pd.DataFrame(tratamento, 
                columns = ['COD', 'PRODUTO', 'QTDE', 'CUSTO_MEDIO', 'MED_NF_ENT','VLR_UNIT', 'VLR_ORIGINAL', 'VLR_UNIT_CORRIG',
                          'VLR_CORRIGIDO', 'EMPRESA', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 'COD_FORNECEDOR',
                          'IDCC','CENTRO_CUSTOS', 'IDCLVL','CIDADE', 'IDCONTA','CONTA', 'DETALHAMENTO'])
        
        # Acrescentando os valores das colunas que tiveram apenas os nomes alterados
        tratamento['DATA'] = entrada['DATA_MOV']
        tratamento['DOCUMENTO'] = entrada['OBS_ID_OS']
        
        tratamento['HISTORICO'] = entrada['PRODUTO']+" "+ entrada['DESC_PRINC']
        
        # Construção do código IDCC seguindo o padrão do BD e utilizando os valores da coluna D3_CC da tabela de entrada
        tratamento["IDCC"] = "102"+entrada['FILIAL'].astype(str).str.zfill(4).str[:2]+entrada["D3_CC"].astype(str).str[:7]
        
        # Criando um banco de dados auxiliar para unir os valores criados do IDCC com o banco de dados de tratamento
        auxcc = pd.DataFrame(centro_custos,columns=["idcc","centro_custos"])
        auxcc = pd.merge(tratamento,auxcc,left_on="IDCC",right_on="idcc")
        auxcc = pd.DataFrame(auxcc,columns=["idcc","centro_custos"])
        
        # Construindo um dicionário com os valores de idcc e centro de custos para utilizar no preenchimento da coluna centro_custos
        dic_cc = auxcc.set_index('idcc')['centro_custos'].to_dict()
        
        # Preenchimento da coluna de centro_custos com os valores do dicionário criado para cada valor correspondente na coluna IDCC
        tratamento['CENTRO_CUSTOS'] = tratamento['IDCC'].map(dic_cc)
            
        # Preenchimento da coluna de detalhamento com os valores do dicionário criado para cada valor correspondente na coluna produto
        tratamento['DETALHAMENTO'] = tratamento['PRODUTO'].map(DETALHAMENTO_PRODUTOS)
        
        # Construção do código IDCLVL seguindo o padrão do BD e utilizando os valores da coluna D3_CLVL da tabela de entrada
        tratamento['IDCLVL'] = "102"+"00"+entrada['D3_CLVL'].astype(str).str[:7]
        
        # Criando um banco de dados auxiliar para unir os valores criados do IDCLVL com o banco de dados de tratamento
        auxcid = pd.DataFrame(cidades,columns=["idclvl","classe_valor"])
        auxcid = pd.merge(tratamento,auxcid,left_on="IDCLVL",right_on="idclvl")
        auxcid = pd.DataFrame(auxcid, columns = ["idclvl","classe_valor"])
        
        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna cidade
        dic_cid = auxcid.set_index('idclvl')['classe_valor'].to_dict()
        
        # Preenchimento da coluna de cidade com os valores do dicionário criado para cada valor correspondente na coluna IDCLVL
        tratamento['CIDADE'] = tratamento['IDCLVL'].map(dic_cid)
        
        # Construção do código IDCONTA seguindo o padrão do BD e utilizando os valores da coluna D3_CC da tabela de entrada
        tratamento['IDCONTA'] = "102"+"00"+entrada['CONTA_RESULTADO'].astype(str).str[:11]
        
        # Criando um banco de dados auxiliar para unir os valores criados do IDCONTA com o banco de dados de tratamento
        auxconta = pd.DataFrame(plano_contas,columns=["idconta","conta_contabil"])
        auxconta = pd.merge(tratamento,auxconta,left_on="IDCONTA",right_on="idconta")
        auxconta = pd.DataFrame(auxconta, columns = ["idconta","conta_contabil"])
            
        # Construindo um dicionário com os valores de idconta e conta contabil para utilizar no preenchimento da coluna conta
        dic_conta = auxconta.set_index('idconta')['conta_contabil'].to_dict()
        
        # Preenchimento da coluna de conta com os valores do dicionário criado para cada valor correspondente na coluna IDCONTA
        tratamento['CONTA'] = tratamento['IDCONTA'].map(dic_conta)
        
        # Criando o DataFrame de saída organizando as colunas que serão utilizadas dos dados de tratamento
        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA','EMPRESA', 'DATA', 'VALOR_REF','DOCUMENTO','HISTORICO',
                                        'COD_FORNECEDOR', 'CENTRO_CUSTOS','CIDADE','CONTA','DETALHAMENTO'])
        
        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])
        
        # Colocando valor padrão na coluna FONTE
        saida['FONTE'] = 'MOVIMENTACAO ESTOQUE'
        
        # Colocando valor padrão na coluna DIRETO_CSC
        saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        
        # Colocando valor padrão na coluna TIPO_RATEIO
        saida['TIPO_RATEIO'] = 'OK'
        
        # Colocando valor padrão na coluna MULTIPLICADOR
        saida['MULTIPLICADOR'] = -1
        
        # Transformando o valor para preencer a coluna VALOR_REALIZADO
        saida['VALOR_REALIZADO'] = saida['VALOR_REF']*saida['MULTIPLICADOR']
        
        # Salve o DataFrame em um arquivo Excel em memória
        return salvar_em_excel(saida, "movimentacao_estoque.xlsx")

with st.container(): # Funções e botões para o processamento do arquivo

    coluna1, coluna2 = st.columns(2)    
    with coluna1:     
        # Criar um campo de upload
        uploaded_file = st.file_uploader("Selecione um arquivo Excel", type="xlsx")
                    
    # Nome padrão do arquivo
    nome_padraof = f'movimentacao_estoque_DRE_{mes}_{ano}.xlsx'

    with coluna2:
        st.write('Quando o processo estiver finalizado, aparecerá um botão para clicar e baixar o arquivo')
        if uploaded_file:
            # Processar o arquivo e obter o arquivo Excel processado em memória
            processed_file = processar_movimentacao(uploaded_file)

            # Exibir uma mensagem de sucesso
            st.success('Processamento concluído com sucesso!')

            # Botão para baixar o arquivo processado
            st.download_button(
                label="Baixar Arquivo Processado",
                data=processed_file.getvalue(),
                file_name=nome_padraof,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        
