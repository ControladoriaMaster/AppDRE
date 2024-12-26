from flask import Blueprint, request, render_template, send_file
import pandas as pd
import io
from utils.database import get_connection, close_connection, DETALHAMENTO_PRODUTOS

estoque_blueprint = Blueprint('estoque', __name__, template_folder='../templates')

@estoque_blueprint.route('/estoque', methods=['GET', 'POST'])
def movimentacao_estoque():
    if request.method == 'POST':
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
    
    # Para método GET, exibir o formulário
    return render_template('estoque.html')

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
    finally:
        close_connection(conn)

    return tables

def processar_movimentacao(uploaded_file, mes, ano):
    # Carregar os dados auxiliares
    tables = carregar_dados()
    cidades = tables.get("cidades", pd.DataFrame())
    centro_custos = tables.get("centro_custos", pd.DataFrame())
    plano_contas = tables.get("plano_contas", pd.DataFrame())

    # Carregar o arquivo Excel
    entrada = pd.read_excel(uploaded_file)
     
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
                                        'COD_FORNECEDOR', 'CENTRO_CUSTOS','CIDADE','CONTA','DETALHAMENTO', 'FONTE', 'OBS',
                                        'DIRETO_CSC','TIPO_RATEIO', 'MULTIPLICADOR','VALOR_REALIZADO'])
       
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
    
    # Salvar em Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        saida.to_excel(writer, index=False)
    return output.getvalue()