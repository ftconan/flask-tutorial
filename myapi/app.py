# coding=utf-8
"""
@author: conan
@date:2018/9/14
"""
from flask import Flask, Blueprint
from flask_restful import Api, Resource
from myapi.resources.foo import Foo
# from myapi.resources.bar import Bar
# from myapi.resources.baz import Baz

app = Flask(__name__)
api_bp = Blueprint('api', __name__)
api = Api(api_bp)

api.add_resource(Foo, '/Foo', '/Foo/<str:id>')
# api.add_resource(Bar, '/Bar', '/Bar/<str:id>')
# api.add_resource(Baz, '/Baz', '/Baz/<str:id>')


class TodoItem(Resource):
    """
    todo item
    """
    def get(self, id):
        return {'task': 'Say "Hello, World!"'}


api.add_resource(TodoItem, '/todos/<int:id>')
app.register_blueprint(api_bp)
