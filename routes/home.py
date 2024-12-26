from flask import Blueprint, render_template
from utils.database import get_connection, load_movements

home_blueprint = Blueprint('home', __name__, template_folder='../templates')

@home_blueprint.route('/home')
def home():
    tables = load_movements()
    return render_template('home.html', tables=tables)
