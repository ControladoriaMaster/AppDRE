from flask import Blueprint, request, render_template, send_file
import pandas as pd
import numpy as np
import io
from datetime import datetime
from utils.database import get_connection, close_connection

# Define o blueprint
pagamentos_blueprint = Blueprint('pagamentos', __name__, template_folder='../templates', url_prefix='/pagamentos')

@pagamentos_blueprint.route('/', methods=['GET', 'POST'], strict_slashes=False)
def movimentacao_pagamentos():
    if request.method == 'POST':
        try:
            # Verificar se os campos de mês, ano e arquivo foram preenchidos
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
            processed_file = processar_pagamentos(uploaded_file, mes, ano)

            # Nome do arquivo processado
            nome_arquivo = f'pagamentos_DRE_{mes}_{ano}.xlsx'

            # Retornar o arquivo processado para download
            return send_file(
                io.BytesIO(processed_file),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=nome_arquivo
            )

        except Exception as e:
            return f"Erro ao processar o arquivo: {e}", 500

    # Para método GET, exibir o formulário
    return render_template('pagamentos.html')

def carregar_dados():
    """
    Carrega os dados auxiliares do banco de dados.
    """
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

def processar_pagamentos(uploaded_file, mes, ano):
    """
    Processa o arquivo enviado, aplicando as transformações e salvando o resultado.
    """
    try:
        # Carregar os dados auxiliares
        tables = carregar_dados()
        if not tables:
            raise Exception("Erro ao carregar os dados auxiliares.")

        cidades = tables.get("cidades", pd.DataFrame())
        centro_custos = tables.get("centro_custos", pd.DataFrame())
        plano_contas = tables.get("plano_contas", pd.DataFrame())

        # Carregar o arquivo Excel
        abas = pd.read_excel(uploaded_file, sheet_name=['MONTES CLAROS', 'CSC'])

        # Combinar todas as abas
        entrada = pd.concat([df.assign(CIDADE=nome) for nome, df in abas.items()], ignore_index=True)

        # Criar o DataFrame de tratamento
        tratamento = pd.DataFrame(columns=[
            'IDCC', 'IDCLVL', 'IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO',
            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'OBS'
        ])

        tratamento['DATA'] = entrada['BAIXA']
        tratamento['EMPRESA'] = entrada['FILIAL']
        tratamento['VALOR_REF'] = entrada['VALOR']
        tratamento = tratamento[tratamento['VALOR_REF'] != 0]  # Excluir valores zero
        tratamento['CIDADE'] = entrada['CIDADE']
        tratamento['HISTORICO'] = (
            entrada['NUMERO'].astype(str) +
            ' PAGAMENTOS RW ' + entrada['NATUREZA'] +
            ' CODFORNE ' + entrada['CODFORNE'].astype(str) +
            ' FORNE ' + entrada['NOMFORNE']
        )
        tratamento['COD_FORNECEDOR'] = entrada['CODFORNE']
        tratamento['CONTA'] = entrada['NATUREZA'].str[7:]
        tratamento['DETALHAMENTO'] = entrada['NOMFORNE']
        tratamento['OBS'] = entrada['OBSERV']

        # Ajustes e exclusões
        tratamento['CONTA'] = np.where(tratamento['CONTA'] == 'ENERGIA ELETRICA', 'ENERGIA ELETRICA SG&A', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(tratamento['CONTA'] == 'TAXA EXPEDIENTE', 'INSS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(tratamento['CONTA'] == 'IRRF PESSOAL', 'RETIRADA PRO-LABORE', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(tratamento['CONTA'] == 'ASSISTENCIA MEDICA/ODONTO', 'ASSISTENCIA ODONTO/MEDICA', tratamento['CONTA'])
        tratamento = tratamento[tratamento['CONTA'] != 'EMPRESTIMO PRONAMPE']

        # Criar o DataFrame de saída
        saida = tratamento.copy()
        saida['FONTE'] = 'PAGAMENTOS'
        saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        saida['TIPO_RATEIO'] = 'OK'
        saida['MULTIPLICADOR'] = -1
        saida['VALOR_REALIZADO'] = saida['VALOR_REF'] * saida['MULTIPLICADOR']

        # Salvar o DataFrame em um arquivo Excel em memória
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida.to_excel(writer, index=False)
        return output.getvalue()

    except Exception as e:
        raise
