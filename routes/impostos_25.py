from flask import Blueprint, request, render_template, send_file
import pandas as pd
import numpy as np
import io
from datetime import datetime
from utils.database import get_connection, close_connection

# Define o blueprint
impostos_25_blueprint = Blueprint('impostos_25', __name__, template_folder='../templates', url_prefix='/impostos')

@impostos_25_blueprint.route('/', methods=['GET', 'POST'], strict_slashes=False)
def processar_impostos():
    if request.method == 'POST':
        try:
            mes = request.form.get('mes')
            ano = request.form.get('ano')

            if not mes or not ano:
                return "Por favor, preencha os campos de mês e ano.", 400

            if 'file' not in request.files:
                return "Nenhum arquivo enviado!", 400

            uploaded_file = request.files['file']
            if uploaded_file.filename == '':
                return "Arquivo inválido!", 400

            # Processar o arquivo
            processed_file = process_excel_faturamento(uploaded_file, mes, ano)

            nome_arquivo = f'faturamento_impostos_DRE_{mes}_{ano}.xlsx'

            return send_file(
                io.BytesIO(processed_file),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=nome_arquivo
            )

        except Exception as e:
            return f"Erro ao processar o arquivo: {e}", 500

    return render_template('impostos.html')

def carregar_dados():
    queries = {
        "cidades": "SELECT * FROM protheus.dim_classe_valor;",
        "centro_custos": "SELECT * FROM protheus.dim_centro_custos;",
        "plano_contas": "SELECT * FROM protheus.dim_plano_contas;"
    }

    conn = get_connection()
    if not conn:
        return {}

    try:
        tables = {}
        cursor = conn.cursor()
        for key, query in queries.items():
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            tables[key] = pd.DataFrame(data, columns=columns)
        return tables
    except Exception as e:
        return {}
    finally:
        close_connection(conn)

def preencher_data_por_mes(entrada, mes, nome_coluna_data, ano):
    meses = {
        'Janeiro': '01-15', 'Fevereiro': '02-15', 'Março': '03-15',
        'Abril': '04-15', 'Maio': '05-15', 'Junho': '06-15',
        'Julho': '07-15', 'Agosto': '08-15', 'Setembro': '09-15',
        'Outubro': '10-15', 'Novembro': '11-15', 'Dezembro': '12-15'
    }

    if mes not in meses:
        raise ValueError("Nome do mês inválido")

    data_string = f"{ano}-{meses[mes]}"
    entrada[nome_coluna_data] = pd.to_datetime(data_string, format="%Y-%m-%d")

def process_excel_faturamento(uploaded_file, mes, ano):
    try:
        tables = carregar_dados()
        cidades = tables.get("cidades", pd.DataFrame())

        entrada = pd.read_excel(uploaded_file)
        entrada = pd.DataFrame(entrada, columns=['CIDADE', 'ESTRATIFICADO', 'VALOR', 'GRUPO', 'DATA'])

        # Preencher a coluna DATA com o mês informado no início do relatório
        preencher_data_por_mes(entrada, mes, 'DATA', ano)

    # Tratamento do Dados de Faturamento
        tratamento = pd.DataFrame(entrada, columns = ['CIDADE','IDCLVL','DATA','VALOR', 'FILIAL', 'CONTA', 'DESCRICAO'])
        tratamento['CIDADE'] = np.where(tratamento['CIDADE'] == 'DIVINOPOLIS', 'DIVINOPOLIS REGIONAL', tratamento['CIDADE'])

        auxcid = cidades[cidades['base'] == 'ProtheusSA'].copy()
        auxcid = pd.DataFrame(auxcid,columns=["idclvl","classe_valor"])
        auxcid = pd.merge(tratamento,auxcid,left_on="CIDADE",right_on="classe_valor")
        auxcid = pd.DataFrame(auxcid,columns=["idclvl","classe_valor"])

        dic_cid = auxcid.set_index('classe_valor')['idclvl'].to_dict()

        tratamento['IDCLVL'] = tratamento['CIDADE'].map(dic_cid)

        tratamento ['FILIAL'] = entrada ['GRUPO'].str.upper()

        tratamento ['DESCRICAO'] = entrada ['ESTRATIFICADO'].str.upper()

        # Os valore da coluna CONTA são preenchidos com base nos valores de referência da coluna ESTRATIFICADO do banco de entrada.
        # Porém, os valores devem ser transformados e agrupados de acordo com os tipos de conta disponíveis.
        # Valores autalizados - Alteração principal no código está aqui.
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'INSTALACAO', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Lançamentos Financeiros', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Locação', 'LOCAÇÃO DE BENS E MÓVEIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master Resolve', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Mensalidade Pay TV', 'NF MENSALIDADE', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PACOTE SUPORTE AVANÇADO', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PSCI', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SCM', 'NF SCM', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SCM SOB MVNO', 'NF SCM', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SERVICO DE VALOR ADICIONAL', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SERVIÇOS DIGITAIS', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Serviços Técnicos', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SVA sobre MVNO', 'SVA SOBRE MVNO', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'UBOOK 1', 'UBOOK 1', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'UBOOK 2', 'UBOOK 2', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'UBOOK 3', 'UBOOK 3', tratamento['CONTA'])

        # Criando o DataFrame para o tratamento dos impostos 
        tratamento_impostos = pd.DataFrame(tratamento, columns = ['CIDADE', 'IDCLVL', 'DATA', 'VALOR', 'FILIAL', 'CONTA', 'DESCRICAO', 
                                                        'ICMS', 'PIS', 'COFINS', 'FUST', 'FUNTTEL', 'CSLL', 'IR'])

        class Imposto:
            def __init__(self, aliquota):
                self.aliquota = aliquota

            def aplicar_imposto(self, valor):
                return valor * (self.aliquota / 100)
        
        tratamento_impostos['ICMS'] = tratamento_impostos['CONTA'].apply(
        lambda x: '10' if x == 'NF MENSALIDADE' else ('18' if x == 'NF SCM' else None)
        )

        icms_10 = Imposto(aliquota=10)
        icms_18 = Imposto(aliquota=18)

        tratamento_impostos['VALOR_ICMS'] = tratamento_impostos.apply(
            lambda row: icms_10.aplicar_imposto(row['VALOR']) if row['ICMS'] == '10'
            else icms_18.aplicar_imposto(row['VALOR']) if row['ICMS'] == '18'
            else 0, axis=1
        )

        tratamento_impostos['PIS'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE' 
                                                                        or x == 'VENDAS DE INTERNET' 
                                                                        or x == 'LOCAÇÃO DE BENS E MÓVEIS'or x == 'SERVIÇOS DIGITAIS'
                                                                        or x == 'SERVIÇOS COMPLEMENTARES' or x == 'SVA SOBRE MVNO' )

        pis = Imposto(aliquota=0.65)

        tratamento_impostos['VALOR_PIS'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS']

        tratamento_impostos['VALOR_PIS'] = tratamento_impostos.apply(lambda row: pis.aplicar_imposto(row['VALOR_PIS']) if row['PIS'] 
                                                                else row['PIS'], axis=1)

        tratamento_impostos['COFINS'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE' 
                                                                            or x == 'VENDAS DE INTERNET' 
                                                                            or x == 'LOCAÇÃO DE BENS E MÓVEIS'or x == 'SERVIÇOS DIGITAIS'
                                                                            or x == 'SERVIÇOS COMPLEMENTARES' or x == 'SVA SOBRE MVNO' )
        
        cofins = Imposto(aliquota=3)

        tratamento_impostos['VALOR_COFINS'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS']

        tratamento_impostos['VALOR_COFINS'] = tratamento_impostos.apply(lambda row: cofins.aplicar_imposto(row['VALOR_COFINS']) 
                                                                    if row['COFINS'] else row['COFINS'], axis=1)

        tratamento_impostos['FUST'] = tratamento_impostos['DESCRICAO'].apply(lambda x: x == 'MASTER RESOLVE' or x == 'MENSALIDADE PAY TV'  
                                                                     or x == 'SCM' or x == 'SCM SOB MVNO' 
                                                                     or x == 'SERVICO DE VALOR ADICIONAL' 
                                                                     or x == 'SERVIÇOS TÉCNICOS' or x == 'SVA SOBRE MVNO')
        
        fust = Imposto(aliquota=1)

        tratamento_impostos['VALOR_FUST'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS'] - tratamento_impostos['VALOR_PIS'] - tratamento_impostos['VALOR_COFINS']

        tratamento_impostos['VALOR_FUST'] = tratamento_impostos.apply(lambda row: fust.aplicar_imposto(row['VALOR_FUST']) 
                                                                    if row['FUST'] else row['FUST'], axis=1)

        tratamento_impostos['FUNTTEL'] = tratamento_impostos['DESCRICAO'].apply(lambda x: x == 'MASTER RESOLVE' or x == 'MENSALIDADE PAY TV'  
                                                                     or x == 'SCM' or x == 'SCM SOB MVNO' 
                                                                     or x == 'SERVICO DE VALOR ADICIONAL' 
                                                                     or x == 'SERVIÇOS TÉCNICOS' or x == 'SVA SOBRE MVNO')

        funttel = Imposto(aliquota=0.5)

        tratamento_impostos['VALOR_FUNTTEL'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS'] - tratamento_impostos['VALOR_PIS'] - tratamento_impostos['VALOR_COFINS']

        tratamento_impostos['VALOR_FUNTTEL'] = tratamento_impostos.apply(lambda row: funttel.aplicar_imposto(row['VALOR_FUNTTEL']) 
                                                                    if row['FUNTTEL'] else row['FUNTTEL'], axis=1)

        combinacoes_csll = {
            ('INSTALACAO', 'ITACOLOMI'),
            ('LANÇAMENTOS FINANCEIROS', 'ITACOLOMI'),
            ('PACOTE SUPORTE AVANÇADO', 'ITACOLOMI'),
            ('SCM', 'ITACOLOMI'),
            ('LANÇAMENTOS FINANCEIROS', 'OMC'),
            ('PSCI', 'OMC'),
            ('SERVIÇOS DIGITAIS', 'OMC'),
            ('UBOOK 1', 'OMC'),
            ('UBOOK 2', 'OMC'),
            ('LANÇAMENTOS FINANCEIROS', 'OP11'),
            ('PSCI', 'OP11'),
            ('SERVIÇOS DIGITAIS', 'OP11'),
            ('UBOOK 1', 'OP11'),
            ('UBOOK 2', 'OP11'),
            ('UBOOK 3', 'OP11'),
            ('LANÇAMENTOS FINANCEIROS', 'ORION'),
            ('PSCI', 'ORION'),
            ('SERVIÇOS DIGITAIS', 'ORION'),
            ('UBOOK 1', 'ORION'),
            ('UBOOK 2', 'ORION'),
            ('UBOOK 3', 'ORION'),
        }

        tratamento_impostos['CSLL'] = tratamento_impostos.apply(
            lambda row: (row['DESCRICAO'], row['FILIAL']) in combinacoes_csll,
            axis=1
        )

        csll = Imposto(aliquota=(32*9)/100)

        tratamento_impostos['VALOR_CSLL'] = tratamento_impostos.apply(lambda row: csll.aplicar_imposto(row['VALOR']) if row['CSLL'] 
                                                                else row['CSLL'], axis=1)

        combinacoes_ir = {
            ('INSTALACAO', 'ITACOLOMI'),
            ('LANÇAMENTOS FINANCEIROS', 'ITACOLOMI'),
            ('PACOTE SUPORTE AVANÇADO', 'ITACOLOMI'),
            ('SCM', 'ITACOLOMI'),
            ('LANÇAMENTOS FINANCEIROS', 'OMC'),
            ('PSCI', 'OMC'),
            ('SERVIÇOS DIGITAIS', 'OMC'),
            ('UBOOK 1', 'OMC'),
            ('UBOOK 2', 'OMC'),
            ('LANÇAMENTOS FINANCEIROS', 'OP11'),
            ('PSCI', 'OP11'),
            ('SERVIÇOS DIGITAIS', 'OP11'),
            ('UBOOK 1', 'OP11'),
            ('UBOOK 2', 'OP11'),
            ('UBOOK 3', 'OP11'),
            ('LANÇAMENTOS FINANCEIROS', 'ORION'),
            ('PSCI', 'ORION'),
            ('SERVIÇOS DIGITAIS', 'ORION'),
            ('UBOOK 1', 'ORION'),
            ('UBOOK 2', 'ORION'),
            ('UBOOK 3', 'ORION'),
        }
                
        tratamento_impostos['IR'] = tratamento_impostos.apply(
            lambda row: (row['DESCRICAO'], row['FILIAL']) in combinacoes_ir,
            axis=1
        )

        adicional_ir_total = tratamento_impostos.loc[tratamento_impostos['IR'] == True, 'VALOR'].sum()
        adicional_ir = (adicional_ir_total*(32/100))-20000
        adicional_ir = ((adicional_ir *10)/adicional_ir_total)/100
        adicional_ir

        tratamento_impostos['ADICIONAL_IR'] = tratamento_impostos.apply(lambda row: row['VALOR'] * adicional_ir if row['IR'] else row['IR'], axis=1)

        ir = Imposto(aliquota=(32*15)/100)

        tratamento_impostos['VALOR_IR'] = tratamento_impostos.apply(lambda row: ir.aplicar_imposto(row['VALOR']) if row['IR'] 
                                                                else row['IR'], axis=1)
        
        tratamento_impostos['VALOR_IR'] = tratamento_impostos['VALOR_IR'] + tratamento_impostos['ADICIONAL_IR']

        df_empilhado = pd.melt(tratamento_impostos, id_vars=['FILIAL','CIDADE', 'IDCLVL', 'DATA', 'CONTA', 'ICMS', 'PIS', 'COFINS', 'FUST', 
                                                            'FUNTTEL', 'CSLL', 'IR'], value_vars=['VALOR_ICMS', 'VALOR_PIS', 
                                                                                                'VALOR_COFINS', 'VALOR_FUST',
                                                                                                'VALOR_FUNTTEL', 'VALOR_CSLL',
                                                                                                'VALOR_IR'], 
                        var_name='NCONTA', value_name='IMPOSTO')
        
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_ICMS') & (df_empilhado['ICMS'] == False))]
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_PIS') & (df_empilhado['PIS'] == False))]
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_COFINS') & (df_empilhado['COFINS'] == False))]
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_FUST') & (df_empilhado['FUST'] == False))]
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_FUNTTEL') & (df_empilhado['FUNTTEL'] == False))]
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_CSLL') & (df_empilhado['CSLL'] == False))]
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_IR') & (df_empilhado['IR'] == 0))]

        # Criar o DataFrame de saída do Faturamento
        saida_faturamento = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])

        saida_faturamento['VALOR_REF'] = tratamento['VALOR']
        saida_faturamento['EMPRESA'] = tratamento['FILIAL']
        saida_faturamento['HISTORICO'] = tratamento['DESCRICAO']
        saida_faturamento['FONTE'] = 'FATURAMENTO E IMPOSTOS'
        saida_faturamento['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        saida_faturamento['TIPO_RATEIO'] = 'OK'
        saida_faturamento['MULTIPLICADOR'] = 1
        saida_faturamento['VALOR_REALIZADO'] = saida_faturamento['VALOR_REF']*saida_faturamento['MULTIPLICADOR']

        # Criar o DataFrame de saída dos Impostos
        saida_impostos = pd.DataFrame(df_empilhado, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])

        saida_impostos['VALOR_REF'] = df_empilhado['IMPOSTO']
        saida_impostos['EMPRESA'] = df_empilhado['FILIAL']
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_ICMS', 'ICMS', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_COFINS', 'COFINS', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_CSLL', 'CSLL', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_FUNTTEL', 'FUNTTEL', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_FUST', 'FUST', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_IR', 'IRPJ', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_PIS', 'PIS', saida_impostos['CONTA'])
        string = ' SOBRE '
        saida_impostos['HISTORICO'] = saida_impostos['CONTA'] +  string  + df_empilhado['CONTA']
        saida_impostos['FONTE'] = 'FATURAMENTO E IMPOSTOS'
        saida_impostos['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        saida_impostos['TIPO_RATEIO'] = 'OK'
        saida_impostos['MULTIPLICADOR'] = -1
        saida_impostos['VALOR_REALIZADO'] = saida_impostos['VALOR_REF']*saida_impostos['MULTIPLICADOR']

        #Criar o DataFrame de Saída Final Concatenado
        saida_faturamento_impostos = pd.concat([saida_faturamento, saida_impostos], axis = 0)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida_faturamento_impostos.to_excel(writer, index=False)

        return output.getvalue()

    except Exception as e:
        raise ValueError(f"Erro no processamento do arquivo: {e}")
