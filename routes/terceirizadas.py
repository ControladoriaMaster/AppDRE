from flask import Blueprint, request, render_template, send_file
import pandas as pd
import numpy as np
import io
from datetime import datetime
from utils.database import get_connection, close_connection

# Define o blueprint
terceirizadas_blueprint = Blueprint('terceirizadas', __name__, template_folder='../templates')

@terceirizadas_blueprint.route('/', methods=['GET', 'POST'], strict_slashes=False)
def processar_terceirizadas():
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
            processed_file = process_excel_terceirizadas(uploaded_file, mes, ano)

            nome_arquivo = f'acompanhamento_terceirizadas_DRE_{mes}_{ano}.xlsx'

            return send_file(
                io.BytesIO(processed_file),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=nome_arquivo
            )

        except Exception as e:
            return f"Erro ao processar o arquivo: {e}", 500

    return render_template('terceirizadas.html')

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

def process_excel_terceirizadas(uploaded_file, mes, ano):
    try:
        tables = carregar_dados()
        cidades = tables.get("cidades", pd.DataFrame())
        plano_contas = tables.get("plano_contas", pd.DataFrame())

        entrada = pd.read_excel(uploaded_file)
        
        entrada['NATUREZA'] = entrada['Tipo despesa'].str[7:]

        tratamento = pd.DataFrame(entrada, columns = ['EMPRESA','CIDADE','Data','Valor'])
        tratamento = tratamento.rename (columns ={'Data': 'DATA', 'Valor': 'VALOR'})
        tratamento ['CIDADE'] = tratamento ['CIDADE'].str.upper()
        tratamento['HISTORICO'] = 'PROVISIONAMENTO'+" "+ entrada['EMPRESA']+" "+ tratamento['CIDADE']+" "+entrada['Classificação Conta']+" "+ entrada['Tipo despesa']+" "+ entrada['Histórico']
        tratamento[['IDCC','CENTRO_CUSTOS']] = entrada['CENTRO DE CUSTO'].str.split('-', expand=True)
        tratamento["IDCC"] = "102"+"00"+tratamento["IDCC"].astype(str).str[:7]
        tratamento = pd.DataFrame(tratamento,columns = ['EMPRESA','CIDADE','DATA','VALOR','HISTORICO', 'IDCC','CENTRO_CUSTOS','IDCONTA'])
        tratamento.loc[tratamento['EMPRESA'] == 'CALL CENTER', 'CONTA'] = 'CALL CENTER'
        tratamento.loc[tratamento['EMPRESA'] != 'CALL CENTER', 'CONTA'] = 'EMPREITEIRAS SG&A'

        auxcon = plano_contas[plano_contas['base'] == 'ProtheusSA'].copy()
        auxcon = pd.DataFrame(auxcon,columns=["idconta","conta_contabil"])
        auxcon = pd.merge(tratamento,auxcon,left_on="CONTA",right_on="conta_contabil")
        auxcon = pd.DataFrame(auxcon,columns=["idconta","conta_contabil"])

        dic_con = auxcon.set_index('conta_contabil')['idconta'].to_dict()
        tratamento['IDCONTA'] = tratamento['CONTA'].map(dic_con)

        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223024-PROCESSO TRABALHISTA', 'PROCESSO TRABALHISTA', entrada['Tipo despesa'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221016-RESCISAO', 'RESCISAO', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221014-ASSISTENCIA MEDICA/ODONTO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221013-COMISSÃO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221006-FERIAS', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221010-FGTS RECISORIO/GRRF', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221004-FGTS/GFIP', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221003-INSS/GPS PESSOAL', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221020-IRRF PESSOAL', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227013-IRRF SERVICO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221012-RETIRADA PRO-LABORE', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221002-SALARIO LÍQUIDO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '224006-SEGURANCA MEDICINA TRABALHO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == 'SEGURO PRESTAMISTA', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221009-VALE ALIMENTAÇÃO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221011-VALE TRANSPORTE', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221017-PENSAO ALIMENTICIA', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221015-SEGURO PESSOAL', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221007-BOLSA ESTÁGIO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221001-ADIANTAMENTO SALARIO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223001-ADIANTAMENTO VIAGEM', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223002-AGUA E ESGOTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223003-ALUGUEL IMOVEL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227002-COFINS', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223006-CONDOMINIO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223007-CONSERVAÇÃO/LIMPEZA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225009-SISTEMA/SOFTWARE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223008-CURSO/TREINAMENTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223009-ALIMENTAÇÃO/CAFE/LANCHE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225006-LOCAÇÃO MAQUINA/EQUIPAMENTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223011-CORRESPONDENCIA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225008-MANUTENÇÃO FROTA/REPARO/MAQUINA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225007-LOCAÇÃO FROTA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '229001-IPTU', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '230001-JUROS/MULTA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '230004-IOF', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223015-ENERGIA ELETRICA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '222006-EPI', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223017-FRETE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225001-CONSULTORIA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227004-CSLL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == 'EMPRESTIMOS', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223015-ENERGIA ELETRICA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225005-HONORARIO ADVOCATICIO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225004-HONORARIO CONTABIL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227005-IRPJ', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227014-ISSQN', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '224003-MATERIAL CONSTRUCAO/REFORMA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '224004-MATERIAL ESCRITORIO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223020-MENSALIDADE ASSOCIACAO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227016-PARCELAMENTO IMPOSTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227001-PIS', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227014-ISSQN', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223023-PROCESSO CIVIL/CLIENTE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227018-SIMPLES NACIONAL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '230002-TARIFA MANUTENCAO CONTA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '228006-TAXA EXPEDIENTE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '228001-TAXA CONSELHO PROFISSIONAL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223027-TELEFONIA FIXA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223026-TELEFONIA MOVEL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == 'MULTA MUNICIPAL', 'DESPESAS', tratamento['NATUREZA'])
        
        # Utilizar esse ultimo código apenas se for agrupar o CALL CENTER por despesas, caso queira que apareça as descrições da natureza
        # conforme aparece na EMPREITEIRA SG&A, caso contrário, não utilizar essa linha de código.
        tratamento['NATUREZA'] = np.where(tratamento['EMPRESA'] == 'CALL CENTER', 'DESPESAS', tratamento['NATUREZA'])
        
        tratamento['NCONTA'] = tratamento['CONTA']+" "+tratamento['NATUREZA']
        tratamento['NCONTA'] = np.where(tratamento['NCONTA'] == 'CALL CENTER DESPESAS', 'DESPESAS COM CALL CENTER', tratamento['NCONTA'])
        tratamento['IDCLVL'] = pd.Series(dtype='float')
        tratamento['NCIDADE'] = tratamento.apply(lambda row: row['CIDADE'] if row['EMPRESA'] == 'CALL CENTER' else row['CIDADE'], axis=1 )
        tratamento['NCIDADE'] = np.where(tratamento['EMPRESA'] == 'CALL CENTER', 'CSC', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'DIVINÓPOLIS', 'DIVINOPOLIS REGIONAL', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'POÇOS DE CALDAS', 'POCOS DE CALDAS', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'SÃO SEBASTIÃO DO PARAÍSO', 'SAO SEBASTIAO DO PARAISO', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'TRÊS CORAÇÕES', 'TRES CORACOES', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'IGARAPÉ', 'IGARAPE', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'ITAÚNA', 'ITAUNA', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'PARÁ DE MINAS', 'PARA DE MINAS', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'TAUBATÉ', 'TAUBATE', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'CAMPOS DO JORDÃO', 'CAMPOS DO JORDAO', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'UNAÍ', 'UNAI', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'ITAJUBÁ', 'ITAJUBA', tratamento['NCIDADE'])

        auxcid = cidades[cidades['base'] == 'ProtheusSA'].copy()
        auxcid = pd.DataFrame(auxcid,columns=["idclvl","classe_valor"])
        auxcid = pd.merge(tratamento,auxcid,left_on="NCIDADE",right_on="classe_valor")
        auxcid = pd.DataFrame(auxcid,columns=["idclvl","classe_valor"])

        dic_cid = auxcid.set_index('classe_valor')['idclvl'].to_dict()

        tratamento['IDCLVL'] = tratamento['NCIDADE'].map(dic_cid)
        
        # Criando o DataFrame de saída
        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])

        saida['DATA'] = tratamento['DATA']
        saida['VALOR_REF'] = tratamento['VALOR']
        saida['HISTORICO'] = tratamento['HISTORICO']
        saida['CENTRO_CUSTOS'] = tratamento['CENTRO_CUSTOS']
        saida['CIDADE'] = tratamento['NCIDADE']
        saida['CONTA'] = tratamento['NCONTA']
        saida['DETALHAMENTO'] = tratamento['EMPRESA']
        saida['FONTE'] = 'ACOMPANHAMENTO TERCEIRIZADAS'
        saida['OBS'] = entrada['NATUREZA']
        saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        
        # Colocandos os dados tratados na coluna TIPO_RATEIO
        saida['TIPO_RATEIO'] = np.where(saida['CIDADE'] == 'CSC', 'TOTAL SEM MOC', saida['CIDADE'])
        saida['TIPO_RATEIO'] = np.where(saida['CIDADE'] != 'CSC', 'OK', saida['CIDADE'])
        saida['TIPO_RATEIO'] = np.where(saida['TIPO_RATEIO'] == 'CSC', 'TOTAL SEM MOC', saida['TIPO_RATEIO'])

        saida['MULTIPLICADOR'] = -1
        saida['VALOR_REALIZADO'] = saida['VALOR_REF']*saida['MULTIPLICADOR']

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida.to_excel(writer, index=False)

        return output.getvalue()

    except Exception as e:
        raise ValueError(f"Erro no processamento do arquivo: {e}")
