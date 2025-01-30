from flask import Blueprint, request, render_template, send_file
import pandas as pd
import io
from utils.database import get_connection, close_connection, DETALHAMENTO_PRODUTOS

# Define o blueprint
estoque_blueprint = Blueprint('estoque', __name__, template_folder='../templates', url_prefix='/estoque')

@estoque_blueprint.route('/', methods=['GET', 'POST'], strict_slashes=False)
def movimentacao_estoque():
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
            processed_file = processar_movimentacao(uploaded_file, mes, ano)

            # Nome do arquivo processado
            nome_arquivo = f'movimentacao_estoque_DRE_{mes}_{ano}.xlsx'

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
    return render_template('estoque.html')

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

def processar_movimentacao(uploaded_file, mes, ano):
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
        entrada = pd.read_excel(uploaded_file)

        # Validar colunas obrigatórias
        if 'B1_XCTB' not in entrada.columns:
            raise Exception("Coluna 'B1_XCTB' não encontrada no arquivo enviado.")

        # Excluir registros indesejados
        entrada = entrada[entrada['B1_XCTB'] == 'S']

        # Tratamento inicial dos dados
        tratamento = pd.DataFrame(entrada, columns=[
            'COD', 'PRODUTO', 'QTDE', 'CUSTO_MEDIO', 'MED_NF_ENT',
            'DATA_MOV', 'OBS_ID_OS'
        ])
        tratamento['DATA'] = entrada['DATA_MOV']
        tratamento['DOCUMENTO'] = entrada['OBS_ID_OS']
        tratamento['HISTORICO'] = entrada['PRODUTO'] + " " + entrada['DESC_PRINC']
        tratamento["IDCC"] = "102" + entrada['FILIAL'].astype(str).str.zfill(4).str[:2] + entrada["D3_CC"].astype(str).str[:7]

        # Mapear centro de custos
        auxcc = pd.merge(
            tratamento,
            centro_custos.rename(columns={"idcc": "IDCC", "centro_custos": "CENTRO_CUSTOS"}),
            on="IDCC",
            how="left"
        )
        tratamento['CENTRO_CUSTOS'] = auxcc['CENTRO_CUSTOS']

        # Mapear detalhamento de produtos
        tratamento['DETALHAMENTO'] = tratamento['PRODUTO'].map(DETALHAMENTO_PRODUTOS)

        # Construção do código IDCLVL seguindo o padrão do BD
        tratamento['IDCLVL'] = "102" + "00" + entrada['D3_CLVL'].astype(str).str[:7]
        auxcid = pd.DataFrame(cidades, columns=["idclvl", "classe_valor"])
        auxcid = pd.merge(tratamento, auxcid, left_on="IDCLVL", right_on="idclvl")
        dic_cid = auxcid.set_index('idclvl')['classe_valor'].to_dict()
        tratamento['CIDADE'] = tratamento['IDCLVL'].map(dic_cid)

        # Construção do código IDCONTA
        tratamento['IDCONTA'] = "102" + "00" + entrada['CONTA_RESULTADO'].astype(str).str[:11]
        auxconta = pd.DataFrame(plano_contas, columns=["idconta", "conta_contabil"])
        auxconta = pd.merge(tratamento, auxconta, left_on="IDCONTA", right_on="idconta")
        dic_conta = auxconta.set_index('idconta')['conta_contabil'].to_dict()
        tratamento['CONTA'] = tratamento['IDCONTA'].map(dic_conta)

        # Criando o DataFrame de saída
        saida = pd.DataFrame(tratamento, columns=[
            'IDCC', 'IDCLVL', 'IDCONTA', 'EMPRESA', 'DATA', 'VALOR_REF', 'DOCUMENTO',
            'HISTORICO', 'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA',
            'DETALHAMENTO', 'FONTE', 'OBS', 'DIRETO_CSC', 'TIPO_RATEIO',
            'MULTIPLICADOR', 'VALOR_REALIZADO'
        ])

        # Preenchimento de valores padrões
        saida['FONTE'] = 'MOVIMENTACAO ESTOQUE'
        saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        saida['TIPO_RATEIO'] = 'OK'
        saida['MULTIPLICADOR'] = -1
        saida['VALOR_REALIZADO'] = saida['VALOR_REF'] * saida['MULTIPLICADOR']

        # Salvar em Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida.to_excel(writer, index=False)
        return output.getvalue()

    except Exception as e:
        raise
