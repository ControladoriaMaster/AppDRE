import os
from flask import Flask, render_template
from routes.home import home_blueprint
from routes.estoque import estoque_blueprint
from utils.database import get_connection, close_connection

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

# Registro de blueprints
app.register_blueprint(home_blueprint)
app.register_blueprint(estoque_blueprint)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
     # Testar conexão com o banco de dados
    testar_conexao()
    port = int(os.getenv("PORT", 5000))  # Render define a variável PORT
    app.run(host='0.0.0.0', port=port)