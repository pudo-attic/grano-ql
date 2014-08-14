import json
from flask import Blueprint, request

from grano.lib.serialisation import jsonify
from grano.lib.exc import BadRequest
from grano.core import app
from grano.lib.args import object_or_404
from grano.model import Project
from grano.interface import Startup
from grano.ql.query import run
from grano import authz


blueprint = Blueprint('ql', __name__)


@blueprint.route('/api/1/projects/<slug>/query', methods=['GET', 'POST'])
def query(slug):
    project = object_or_404(Project.by_slug(slug))
    authz.require(authz.project_read(project))

    if request.method == 'POST':
        query = request.json
    else:
        query = json.loads(request.args.get('query', 'null'))
    if query is None:
        raise BadRequest('Invalid data submitted')

    eq = run(project, query)
    return jsonify({
        'status': 'ok',
        'query': query,
        'query_parsed': eq.node,
        'result': eq
    })


class Installer(Startup):

    def configure(self, manager):
        app.register_blueprint(blueprint)
