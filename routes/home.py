from flask import Blueprint, render_template
from utils.database import load_movements

# Define o blueprint
home_blueprint = Blueprint('home', __name__, template_folder='../templates')

# Rota para o blueprint home
@home_blueprint.route('/', endpoint='index')
def home():
    try:
        # Tenta carregar os dados usando load_movements
        tables = load_movements()
    except Exception as e:
        # Em caso de erro, exibe uma mensagem de erro no terminal e no log
        print(f"Erro ao carregar os dados: {e}")
        tables = None  # Define tables como None se houver erro

    # Renderiza o template index.html e passa os dados carregados (ou vazio em caso de erro)
    return render_template('index.html', tables=tables)

