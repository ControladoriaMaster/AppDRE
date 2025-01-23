from flask import Blueprint, request, render_template, send_file
import pandas as pd
import numpy as np
import io
from datetime import datetime
from utils.database import get_connection, close_connection

# Define o blueprint
programacao_blueprint = Blueprint('programacao', __name__, template_folder='../templates')

@programacao_blueprint.route('/', methods=['GET', 'POST'])
def movimentacao_programacao():
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
            processed_file = process_excel_programacao(uploaded_file, mes, ano)

            # Nome do arquivo processado
            nome_arquivo = f'conteudo_programacao_DRE_{mes}_{ano}.xlsx'

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
    return render_template('programacao.html')

def preencher_data_por_mes(mes, ano):
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

    if mes not in meses:
        raise ValueError("Mês inválido")

    data_string = f"{ano}-{meses[mes]}"
    return datetime.strptime(data_string, "%Y-%m-%d")

def process_excel_programacao(uploaded_file, mes, ano):
    # Entrando com os dados de conteúdo de programação das cidades
    aba = pd.read_excel(uploaded_file, sheet_name=['Divinopolis', 'Itajuba', 'Itauna', 'Lavras', 'Montes Claros',
                                                   'Passos', 'Pouso Alegre', 'Unai'])

    dfs = {aba_nome: df for aba_nome, df in aba.items()}

    # Construir o DataFrame de entrada
    entrada = pd.concat([df.assign(CIDADE=aba) for aba, df in dfs.items()], ignore_index=True)

    entrada = pd.DataFrame(entrada, columns=['CIDADE', 'Data', 'Pacote', 'Programadora', 'Canal', 'Início', 
                                             'Fim', 'Média', 'Custo Unit.', 'Valor'])

    # Preencher a coluna data com o mês e ano fornecidos
    entrada['Data'] = preencher_data_por_mes(mes, ano)

    # Criando o DataFrame de tratamento aproveitando algumas colunas dos dados de entrada
    tratamento = pd.DataFrame(entrada, columns=['CIDADE', 'Data', 'Valor'])
    tratamento = tratamento.rename(columns={'Data': 'DATA', 'Valor': 'VALOR'})

    # Ajustes específicos para a coluna "CIDADE"
    tratamento['CIDADE'] = tratamento['CIDADE'].str.upper()
    tratamento['CIDADE'] = np.where(tratamento['CIDADE'] == 'DIVINOPOLIS', 'DIVINOPOLIS REGIONAL', tratamento['CIDADE'])

    tratamento['HISTORICO'] = entrada['Programadora']

    # Preenchendo colunas adicionais
    tratamento['CENTRO_CUSTOS'] = 'TV'
    tratamento['CONTA'] = 'CONTEUDO PROGRAMACAO'

    # Criando o DataFrame de saída
    saida = pd.DataFrame(tratamento, columns=['IDCC', 'IDCLVL', 'IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL',
                                              'COD_PRODUTO', 'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 
                                              'DOCUMENTO', 'HISTORICO', 'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE',
                                              'CONTA', 'DETALHAMENTO', 'FONTE', 'OBS', 'DIRETO_CSC', 'TIPO_RATEIO',
                                              'MULTIPLICADOR', 'VALOR_REALIZADO'])

    saida['VALOR_REF'] = tratamento['VALOR']
    saida['DETALHAMENTO'] = tratamento['HISTORICO']
    saida['FONTE'] = 'CONTEUDO DE PROGRAMACAO'
    saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
    saida['TIPO_RATEIO'] = 'OK'
    saida['MULTIPLICADOR'] = -1
    saida['VALOR_REALIZADO'] = saida['VALOR_REF'] * saida['MULTIPLICADOR']

    # Salve o DataFrame em um arquivo Excel em memória
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        saida.to_excel(writer, index=False)

    return output.getvalue()
