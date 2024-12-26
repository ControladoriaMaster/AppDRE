from flask import Flask, render_template
from routes.home import home_blueprint
from routes.estoque import estoque_blueprint

app = Flask(__name__)

# Registro de blueprints
app.register_blueprint(home_blueprint)
app.register_blueprint(estoque_blueprint)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)