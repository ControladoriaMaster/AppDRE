from flask import Blueprint, request, render_template, send_file, Flask
import pandas as pd
import numpy as np
import io

# Configuração do Flask para aumentar o tamanho máximo de upload
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB de limite para uploads

# Define o blueprint
uniao_blueprint = Blueprint('uniao', __name__, template_folder='../templates', url_prefix='/uniao')

@uniao_blueprint.route('/', methods=['GET', 'POST'], strict_slashes=False)
def processar_uniao():
    if request.method == 'POST':
        try:
            mes = request.form.get('mes')
            ano = request.form.get('ano')

            if not mes or not ano:
                return "Por favor, preencha os campos de mês e ano.", 400

            # Capturar os arquivos enviados
            uploaded_files = request.files.getlist('files')

            if not uploaded_files or all(file.filename == '' for file in uploaded_files):
                return "Nenhum arquivo encontrado!", 400

            # Processar os arquivos carregados
            processed_file = process_excel_uniao(uploaded_files)

            nome_arquivo = f'Arquivo_DRE_{mes}_{ano}.xlsx'

            return send_file(
                io.BytesIO(processed_file),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=nome_arquivo
            )

        except Exception as e:
            return f"Erro ao processar os arquivos: {e}", 500

    return render_template('uniao.html')

def process_excel_uniao(uploaded_files):
    try:
        # Ler e concatenar os arquivos
        dfs = [pd.read_excel(file) for file in uploaded_files]
        concatenado = pd.concat(dfs, ignore_index=True)

        # Salvar o arquivo concatenado em memória antes de leitura para verificar a saída
        output_temp = io.BytesIO()
        with pd.ExcelWriter(output_temp, engine='xlsxwriter') as writer:
            concatenado.to_excel(writer, index=False)
        output_temp.seek(0)  # Retornar ao início do buffer

        # Ler o conteúdo processado para garantir a integridade dos dados
        entrada = pd.read_excel(output_temp)

        # Verificar se as colunas necessárias estão no DataFrame
        colunas_necessarias = ['DIRETO_CSC', 'TIPO_RATEIO', 'FONTE', 'CIDADE', 'VALOR_REF']
        for col in colunas_necessarias:
           if col not in entrada.columns:
                raise ValueError(f"Coluna ausente no arquivo: {col}")

        # Valor para DIRETO_CSC
        valor_direto_csc = 'CSC / ESTRATÉGIA'

        # Preenchendo valores DIRETO_CSC da Fonte DESPESAS CONTÁBIL e CIDADE for igual a CSC
        entrada['DIRETO_CSC'] = np.where((entrada['DIRETO_CSC'].isnull()) & 
                                            (entrada['TIPO_RATEIO'].isnull()) & 
                                            (entrada['FONTE'] == 'DESPESAS CONTÁBIL') &
                                            (entrada['CIDADE'] == 'CSC'),
                                            valor_direto_csc,
                                            entrada['DIRETO_CSC'])

        # Criando uma lista com valores unicos
        valores_unicos_cidades = entrada['CIDADE'].unique().tolist()
        valores_unicos_cidades.remove('CSC')

        # Valor para DIRETO_CSC
        valor_direto_cidades = 'OPERAÇÃO / REGIONAL'

        # Preenchendo valores DIRETO_CSC da Fonte DESPESAS CONTÁBIL e CIDADE for diferente de CSC
        entrada['DIRETO_CSC'] = np.where((entrada['DIRETO_CSC'].isnull()) & 
                                            (entrada['TIPO_RATEIO'].isnull()) & 
                                            (entrada['FONTE'] == 'DESPESAS CONTÁBIL') &
                                            (entrada['CIDADE'].notnull()) &
                                            (entrada['CIDADE'].isin(valores_unicos_cidades)),
                                            valor_direto_cidades,
                                            entrada['DIRETO_CSC'])
        
        # Valor para TIPO DE RATEIO
        valor_rateio_csc = 'TOTAL SEM MOC'  

        # Preenchendo valores TIPO_RATEIO da Fonte DESPESAS CONTÁBIL e CIDADE for igual a CSC
        entrada['TIPO_RATEIO'] = np.where((entrada['DIRETO_CSC'] == 'CSC / ESTRATÉGIA') & 
                                            (entrada['TIPO_RATEIO'].isnull()) & 
                                            (entrada['FONTE'] == 'DESPESAS CONTÁBIL') &
                                            (entrada['CIDADE'] == 'CSC'),
                                            valor_rateio_csc,
                                            entrada['TIPO_RATEIO'])

        # Valor para TIPO DE RATEIO
        valor_rateio_cidades = 'OK'

        #Preenchendo valores TIPO_RATEIO da Fonte DESPESAS CONTÁBIL e CIDADE for diferente de CSC
        entrada['TIPO_RATEIO'] = np.where((entrada['DIRETO_CSC'] == 'OPERAÇÃO / REGIONAL') & 
                                            (entrada['TIPO_RATEIO'].isnull()) & 
                                            (entrada['FONTE'] == 'DESPESAS CONTÁBIL') &
                                            (entrada['CIDADE'].notnull()) &
                                            (entrada['CIDADE'].isin(valores_unicos_cidades)),
                                            valor_rateio_cidades,
                                            entrada['TIPO_RATEIO'])

        # Alterando o Mulitplicador para Zero
        entrada.loc[entrada['CENTRO_CUSTOS'] == 'PRESIDENTE', 'MULTIPLICADOR'] = 0

        #Criando lista de empresas
        empresas = ['1301-DEVICE COMPANY', '1501-AERO R66 - LOCACOES DE HELICOPTERO SPE L', '0501-KROMA PARTICIPACOES S/A',
                    '1401-SPE VISTA ALEGRE', '1001-ISCA REFLORESTAMENTO LTDA - ME', '2101-MGM PARTICIPACOES EIRELI', '2301-PRIME SERVICE LTDA',
                    'KROMA PARTICIPACOES S/A', 'DEVICE COMPANY', 'SPE VISTA ALEGRE', 'AERO R66 - LOCACOES DE HELICOPTERO SPE L',
                    'ISCA REFLORESTAMENTO LTDA - ME']
    
        # Alterando o Mulitplicador para Zero
        entrada.loc[entrada['EMPRESA'].isin(empresas), 'MULTIPLICADOR'] = 0
        
        #Criando listas de critérios para conta e fonte
        criterio_conta = ['CONTEUDO PROGRAMACAO', 'EMPREITEIRAS SG&A', '13º SALARIO', 'JUROS']
        criterio_fonte = ['DESPESAS CONTÁBIL']
        
        # Alterando o Mulitplicador para Zero
        entrada.loc[(entrada['CONTA'].isin(criterio_conta)) & (entrada['FONTE'].isin(criterio_fonte)), 'MULTIPLICADOR'] = 0
      
        # Criando as colunas do DataFrame
        saida =  pd.DataFrame(columns = ['EMPRESA','DATA','VALOR REF', 'DOCUMENTO', 'HISTORICO', 'COD FORNECEDOR', 'CENTRO DE CUSTO',
                                        'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 'OBS', 'DIRETO/CSC', 'TIPO DE RATEIO', 
                                        'MULTIPLICADOR', 'VALOR REALIZADO'])
    
        saida['EMPRESA'] = entrada['EMPRESA']
        saida['DATA'] = entrada['DATA']
        saida['VALOR REF'] = entrada['VALOR_REF']
        saida['DOCUMENTO'] = entrada['DOCUMENTO']
        saida['HISTORICO'] = entrada['HISTORICO']
        saida['COD FORNECEDOR'] = entrada['COD_FORNECEDOR']
        saida['CENTRO DE CUSTO'] = entrada['CENTRO_CUSTOS']
        saida['CIDADE'] = entrada['CIDADE']
        saida['CONTA'] = entrada['CONTA']
        saida['DETALHAMENTO'] = entrada['DETALHAMENTO']
        saida['FONTE'] = entrada['FONTE']
        saida['OBS'] = entrada['OBS']
        saida['DIRETO/CSC'] = entrada['DIRETO_CSC']
        saida['TIPO DE RATEIO'] = entrada['TIPO_RATEIO']
        saida['MULTIPLICADOR'] = entrada['MULTIPLICADOR']
        # Se a conta for 'SERVICO COBRANCA', definir o multiplicador como -1
        saida.loc[saida['CONTA'] == 'SERVICO COBRANCA', 'MULTIPLICADOR'] = -1
        saida['VALOR REALIZADO'] = saida['VALOR REF'] * saida['MULTIPLICADOR']
        
        #Construindo listas para filtrar o banco de dados
        lista_conta = ['ADIANTAMENTO DE SALARIOS', 'ADICIONAL NOTURNO', 'COMISSAO', 'DESCANSO REMUNERADO', 'FGTS S/ SALARIO', 'HORAS EXTRAS',       
               'INSS', 'ORDENADOS E SALARIOS', 'PERICULOSIDADE', 'PREMIO', 'QUEBRA DE CAIXA']
        
        lista_cc = ['OPERACOES ADMINISTRATIVO REGIONAL', 'OPERACOES COMERCIAL/SHOWROOM REGIONAL', 'OPERACOES TECNICAS REGIONAL', 
            'ADMINISTRACAO DE PESSOAL', 'ANALISE DE CREDITO', 'CANAIS INDIRETOS', 'CENTRO DE SOLUCOES TECNICAS/CST', 
            'CENTRO DISTRIBUICAO', 'CENTRO REPAROS', 'COMERCIAL B2B', 'COMERCIAL B2C', 'CONTABILIDADE/FISCAL', 'CONTROLADORIA/FP&A',
            'EXPERIENCIA DO CLIENTE (CALL CENTER)', 'FACILITIES', 'FINANCEIRO', 'GESTÃO DE RECEBIVEIS', 'GESTAO DE RISCOS', 
            'GESTAO DO DESENVOLVIMENTO HUMANO', 'INFRAESTRUTURA DE TI', 'INTELIGENCIA OPERACIONAL', 'IP E SERVICOS', 'JURIDICO', 'LGPD',
            'MARKETING', 'NOC', 'PMO/TI', 'PRODUTOS', 'PROJETOS', 'SUPRIMENTOS', 'TI', 'TRANSMISSAO E INFRAESTUTURA', 
            'AREA TECNICA MCL', 'COMERCIAL B2B MCL', 'COMERCIAL B2C MCL', 'DEPARTAMENTO PESSOAL MCL', 'FINANCEIRO MCL', 
            'GESTAO DE CLIENTES/CALL CENTER MCL', 'IMPLANTACAO/PROJETOS MCL', 'LOGISTICA MCL', 'OPERACOES COMERCIAL/SHOW ROOM MCL',
            'RECURSOS HUMANOS MCL', 'SUPRIMENTOS/FACILITIES MCL']
        
        #Criando um novo banco de dados filtrado
        provisao = saida[(saida['CONTA'].isin(lista_conta)) & (saida['CENTRO DE CUSTO'].isin(lista_cc))]
        
        # Agrupando por 'CIDADE' e 'CENTRO DE CUSTO' e calculando a soma de 'valor'
        provisao_agrupado = provisao.groupby(['CIDADE', 'CENTRO DE CUSTO'])

        # Agrupando por 'col1' e 'col2'
        provisao_agrupado = provisao.groupby(['CIDADE', 'CENTRO DE CUSTO']).agg({
            'VALOR REF': 'sum',  # Soma os valores na coluna 'VALOR REALIZADO'
            'EMPRESA': 'first',
            'DATA': 'first',
        }).reset_index()
        
        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida_prov = pd.DataFrame(provisao_agrupado, columns = ['EMPRESA','DATA','VALOR REF', 'DOCUMENTO', 'HISTORICO', 'COD FORNECEDOR', 'CENTRO DE CUSTO',
                                        'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 'OBS', 'DIRETO/CSC', 'TIPO DE RATEIO', 
                                        'MULTIPLICADOR', 'VALOR REALIZADO'])
        
        saida_prov['VALOR REF'] = saida_prov['VALOR REF']/12
        saida_prov['HISTORICO'] = 'PROVISAO 13º'
        saida_prov['CONTA'] = '13º SALARIO'
        saida_prov['DETALHAMENTO'] = 'PROVISAO 13º'
        saida_prov.loc[saida_prov['CIDADE'] == 'CSC', 'DIRETO/CSC'] = 'CSC / ESTRATÉGIA'
        saida_prov.loc[saida_prov['CIDADE'] != 'CSC', 'DIRETO/CSC'] = 'OPERAÇÃO / REGIONAL'
        saida_prov.loc[saida_prov['CIDADE'] == 'CSC', 'TIPO DE RATEIO'] = 'FOLHA CSC'
        saida_prov.loc[saida_prov['CIDADE'] != 'CSC', 'TIPO DE RATEIO'] = 'OK'
        saida_prov['MULTIPLICADOR'] = -1
        saida_prov['VALOR REALIZADO'] = round(saida_prov['VALOR REF'] * saida_prov['MULTIPLICADOR'], 2)
        
        # Concatenando os DataFrames
        saida_final = pd.concat([saida, saida_prov])

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida_final.to_excel(writer, index=False)

        return output.getvalue()

    except Exception as e:
        raise ValueError(f"Erro no processamento dos arquivos: {e}")
        