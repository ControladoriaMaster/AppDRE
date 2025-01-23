import os
from flask import Flask, render_template
from routes.home import home_blueprint
from routes.estoque import estoque_blueprint
from routes.pagamentos import pagamentos_blueprint
from routes.programacao import programacao_blueprint
from routes.impostos import impostos_blueprint
from routes.terceirizadas import terceirizadas_blueprint
from routes.despesas import despesas_blueprint
from routes.uniao import uniao_blueprint
from utils.database import get_connection, close_connection
import requests

def testar_conexao():
    """
    Testa a conexão com o banco de dados.
    """
    conn = get_connection()
    if conn:
        print("Conexão bem-sucedida com o banco de dados!")
        close_connection(conn)
    else:
        print("Falha ao conectar ao banco de dados.")

app = Flask(__name__)

# Registrar blueprints
app.register_blueprint(home_blueprint, url_prefix='/')
app.register_blueprint(estoque_blueprint, url_prefix='/estoque')
app.register_blueprint(pagamentos_blueprint, url_prefix='/pagamentos')
app.register_blueprint(programacao_blueprint, url_prefix='/programacao')
app.register_blueprint(impostos_blueprint, url_prefix='/impostos')
app.register_blueprint(terceirizadas_blueprint, url_prefix='/terceirizadas')
app.register_blueprint(despesas_blueprint, url_prefix='/despesas')
app.register_blueprint(uniao_blueprint, url_prefix='/uniao')

@app.route('/pagamentos')
def pagamentos():
    return render_template('pagamentos.html')

@app.route('/impostos')
def impostos():
    return render_template('impostos.html')

@app.route('/terceirizadas')
def terceirizadas():
    return render_template('terceirizadas.html')

@app.route('/despesas')
def despesas():
    return render_template('despesas.html')

@app.route('/programacao')
def programacao():
    return render_template('programacao.html')

@app.route('/uniao')
def uniao():
    return render_template('uniao.html')

def obter_ip_publico():
    try:
        response = requests.get('https://api64.ipify.org?format=json')
        if response.status_code == 200:
            ip = response.json().get('ip')
            print(f"IP público do Render: {ip}")
            return ip
        else:
            print(f"Erro ao obter IP público: {response.status_code}")
    except Exception as e:
        print(f"Erro ao conectar ao serviço de IP público: {e}")

if __name__ == '__main__':
    obter_ip_publico()
    # Testar conexão com o banco de dados
    testar_conexao()
    port = int(os.getenv("PORT", 5000))  # Render define a variável PORT

    # Executar o app Flask
    app.run(host='0.0.0.0', port=port, debug=True)
