from flask import Blueprint, request, render_template, send_file
import pandas as pd
import numpy as np
import io
from datetime import datetime
from utils.database import get_connection, close_connection

# Define o blueprint
despesas_blueprint = Blueprint('despesas', __name__, template_folder='../templates')

@despesas_blueprint.route('/', methods=['GET', 'POST'])
def processar_despesas():
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
            processed_file = process_excel_despesas_contabeis(uploaded_file, mes, ano)

            nome_arquivo = f'despesas_contábeis_DRE_{mes}_{ano}.xlsx'

            return send_file(
                io.BytesIO(processed_file),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=nome_arquivo
            )

        except Exception as e:
            return f"Erro ao processar o arquivo: {e}", 500

    return render_template('despesas.html')

def carregar_dados():
    queries = {
        "cidades": "SELECT * FROM protheus.dim_classe_valor;",
        "centro_custos": "SELECT * FROM protheus.dim_centro_custos;",
        "plano_contas": "SELECT * FROM protheus.dim_plano_contas;",
        "fornecedor": "Select * from protheus.dim_fornecedor;",
        "mapeamento": "Select * from datamaster.dre_automatiza_tratamentos;"
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

def process_excel_despesas_contabeis(uploaded_file, mes, ano):
    try:
        tables = carregar_dados()
        cidades = tables.get("cidades", pd.DataFrame())
        plano_contas = tables.get("plano_contas", pd.DataFrame())
        centro_custos = tables.get("centro_custos", pd.DataFrame())
        fornecedor = tables.get("fornecedor", pd.DataFrame())
        mapeamento = tables.get("mapeamento", pd.DataFrame())

        entrada = pd.read_excel(uploaded_file)

        aba = pd.read_excel(uploaded_file, sheet_name=['SA', 'RBC'])

        dfs = {}
        for aba_nome, df in aba.items():
            dfs[aba_nome] = df

        for aba_nome, df in dfs.items():
            print(f"DataFrame da aba '{aba_nome}':")
        
        # Construindo o banco de dados de entrada
        entrada = pd.concat([df.assign(FONTE=aba) for aba, df in dfs.items()], ignore_index=True)

        # Criando o DataFrame de tratamento
        tratamento = pd.DataFrame(entrada, columns = ['Filial','Data Lcto','CtaDebito','Valor', 'Hist Lanc', 'C Custo Deb', 'Item Conta C',
                                          'Cod Cl Val D'])
        
        tratamento.rename(columns={'Filial': 'EMPRESA', 'Data Lcto': 'DATA', 'Hist Lanc': 'HISTORICO', 'Item Conta C': 
                          'COD_FORNECEDOR'}, inplace=True)

        valor = 'ZZZZZZZ'
        tratamento['COD_FORNECEDOR'] = tratamento['COD_FORNECEDOR'].fillna(valor)

        tratamento['BASE'] = np.where(entrada['FONTE'] == 'RBC', 101, entrada['FONTE'])
        tratamento['BASE'] = np.where(entrada['FONTE'] == 'SA', 102, tratamento['BASE'])
        tratamento['EE'] = tratamento["EMPRESA"].astype(str).str[:2]
        tratamento['EE'] = np.where(entrada['FONTE'] == 'SA', '00', tratamento['EE'])
        tratamento['IDCONTA'] = tratamento['BASE'].astype(str) + tratamento['EE'] + tratamento['CtaDebito'].astype(str).str.strip().str[:11]
        tratamento['CONTA'] = ''

        auxcon = pd.DataFrame(plano_contas,columns=["idconta","conta_contabil"])
        auxcon = pd.merge(tratamento,auxcon,left_on="IDCONTA",right_on="idconta")
        auxcon = pd.DataFrame(auxcon,columns=["idconta","conta_contabil"])

        dic_con = auxcon.set_index('idconta')['conta_contabil'].to_dict()

        tratamento['CONTA'] = tratamento['IDCONTA'].map(dic_con)
        tratamento['EEC'] = '00'
        tratamento['IDCLVL'] = tratamento['BASE'].astype(str) + tratamento['EEC'] + tratamento['Cod Cl Val D'].astype(str).str.strip().str[:7]
        valoresidclvl = 'YYYYYYYYYYYY'
        tratamento['IDCLVL'] = tratamento.apply(lambda row: row['IDCLVL'] if pd.notna(row['Cod Cl Val D']) else valoresidclvl, axis=1)

        tratamento['CIDADE'] = ''
        auxcit = pd.DataFrame(cidades,columns=["idclvl","classe_valor"])
        auxcon = pd.merge(tratamento,auxcit,left_on="IDCLVL",right_on="idclvl")
        auxcon = pd.DataFrame(auxcon,columns=["idclvl","classe_valor"])
        dic_cit = auxcit.set_index('idclvl')['classe_valor'].to_dict()
        tratamento['CIDADE'] = tratamento['IDCLVL'].map(dic_cit)

        tratamento['EECC'] = tratamento["EMPRESA"].astype(str).str[:2]
        tratamento['EECC'] = np.where(entrada['FONTE'] == 'RBC', '00', tratamento['EECC'])

        tratamento['IDCC'] = tratamento['BASE'].astype(str) + tratamento['EECC'] + tratamento['C Custo Deb'].astype(str).str.strip().str[:7]
        valoresidcc = 'XXXXXXXXXXXX'
        tratamento['IDCC'] = tratamento.apply(lambda row: row['IDCC'] if pd.notna(row['C Custo Deb']) else valoresidcc, axis=1)

        tratamento['CENTRO_CUSTOS'] = ''
        auxcc = pd.DataFrame(centro_custos,columns=["idcc","centro_custos"])
        auxcc = pd.merge(tratamento,auxcc,left_on="IDCC",right_on="idcc")
        auxcc = pd.DataFrame(auxcc,columns=["idcc","centro_custos"])
        dic_cc = auxcc.set_index('idcc')['centro_custos'].to_dict()
        tratamento['CENTRO_CUSTOS'] = tratamento['IDCC'].map(dic_cc)

        mapeamento['mapeamento'] = mapeamento['idconta'].astype(str) + mapeamento['idfornecedor'].astype(str) + mapeamento['idcc'].astype(str) + mapeamento['idclvl'].astype(str)
        tratamento ['MAPEAMENTO'] = tratamento['IDCONTA'].astype(str) + tratamento['COD_FORNECEDOR'].astype(str).str[:7] + tratamento['IDCC'].astype(str) + tratamento['IDCLVL'].astype(str)

        tratamento['DIRETO_CSC'] = ''

        auxdircsc = pd.DataFrame(mapeamento,columns=["mapeamento","direto_csc"])
        auxdircsc = pd.merge(tratamento,auxdircsc,left_on="MAPEAMENTO",right_on="mapeamento")
        auxdircsc = pd.DataFrame(auxdircsc,columns=["mapeamento","direto_csc"])
        dic_dircsc = auxdircsc.set_index('mapeamento')['direto_csc'].to_dict()
        tratamento['DIRETO_CSC'] = tratamento['MAPEAMENTO'].map(dic_dircsc)

        tratamento['TIPO_RATEIO'] = ''
        auxtrat = pd.DataFrame(mapeamento,columns=["mapeamento","tipo_rateio"])
        auxtrat = pd.merge(tratamento,auxtrat,left_on="MAPEAMENTO",right_on="mapeamento")
        auxtrat = pd.DataFrame(auxtrat,columns=["mapeamento","tipo_rateio"])
        dic_trat = auxtrat.set_index('mapeamento')['tipo_rateio'].to_dict()
        tratamento['TIPO_RATEIO'] = tratamento['MAPEAMENTO'].map(dic_trat)

        tratamento['MULTIPLICADOR'] = ''
        auxmult = pd.DataFrame(mapeamento,columns=["mapeamento","multiplicador"])
        auxmult = pd.merge(tratamento,auxmult,left_on="MAPEAMENTO",right_on="mapeamento")
        auxmult = pd.DataFrame(auxmult,columns=["mapeamento","multiplicador"])
        dic_mult = auxmult.set_index('mapeamento')['multiplicador'].to_dict()
        tratamento['MULTIPLICADOR'] = tratamento['MAPEAMENTO'].map(dic_mult)
        valormult = -1
        tratamento['MULTIPLICADOR'] = tratamento['MULTIPLICADOR'].fillna(valormult)

        tratamento['EEI'] = tratamento["EMPRESA"].astype(str).str[1:4]
        tratamento['IDITEM'] = tratamento['BASE'].astype(str) + tratamento['COD_FORNECEDOR'].astype(str).str.strip().str[:7]

        tratamento['DETALHAMENTO'] = ''
        auxit = pd.DataFrame(fornecedor,columns=["iditem","a2_nome"])
        auxit = pd.merge(tratamento,auxit,left_on="IDITEM",right_on="iditem")
        auxit = pd.DataFrame(auxit,columns=["iditem","a2_nome"])
        dic_it = auxit.set_index('iditem')['a2_nome'].to_dict()
        auxdet = pd.DataFrame(mapeamento,columns=["mapeamento","detalhamento"])
        auxdet = pd.merge(tratamento,auxdet,left_on="MAPEAMENTO",right_on="mapeamento")
        auxdet = pd.DataFrame(auxdet,columns=["mapeamento","detalhamento"])
        dic_det = auxdet.set_index('mapeamento')['detalhamento'].to_dict()
        tratamento['DETALHAMENTO'] = tratamento['MAPEAMENTO'].map(dic_det)
        detalhamentobranco = tratamento['IDITEM'].map(dic_it)
        tratamento['DETALHAMENTO'] = tratamento['DETALHAMENTO'].fillna(detalhamentobranco)

        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                        'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                         'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                        'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])

        saida['VALOR_REF'] = tratamento['Valor']
        saida['FONTE'] = 'DESPESAS CONTÁBIL'
        saida['COD_FILIAL'] = tratamento["EMPRESA"].astype(str).str[:2]
        saida['NOME_FILIAL'] = tratamento["EMPRESA"].astype(str).str[5:]
        saida['MULTIPLICADOR'] = tratamento['MULTIPLICADOR']
        saida['VALOR_REALIZADO'] = saida['VALOR_REF']*saida['MULTIPLICADOR']

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida.to_excel(writer, index=False)

        return output.getvalue()

    except Exception as e:
        raise ValueError(f"Erro no processamento do arquivo: {e}")

