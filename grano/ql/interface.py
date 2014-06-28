from flask import Blueprint

from grano.lib.serialisation import jsonify
from grano.core import app
from grano.interface import Startup

blueprint = Blueprint('ql', __name__)


@blueprint.route('/api/1/query')
def query():
    return jsonify({'status': 'ok'})


class Installer(Startup):

    def configure(self, manager):
        app.register_blueprint(blueprint)
