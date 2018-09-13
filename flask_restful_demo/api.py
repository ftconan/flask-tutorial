# coding=utf-8

"""
@author: magician
@date: 2018/9/11
"""
from flask import Flask, request
from flask_restful import Resource, Api, reqparse, abort

app = Flask(__name__)
api = Api(app)


class HelloWorld(Resource):
    """
    hello world
    """
    def get(self):
        return {'hello': 'world'}


todos = {}


class TodoSimple(Resource):
    """
    resourceful routing
    """
    def get(self, todo_id):
        return {todo_id: todos[todo_id]}

    def put(self, todo_id):
        todos[todo_id] = request.form['data']
        return {todo_id: todos[todo_id]}


class Todo1(Resource):
    """
    response code header message
    """
    def get(self):
        """
        default to 200 0k
        :return:
        """
        return {'task': 'Hello world'}


class Todo2(Resource):
    """
    response code header message
    """
    def get(self):
        """
        Set the response code to 201
        :return:
        """
        return {'task': 'Hello world'}, 201


class Todo3(Resource):
    """
    response code header message

    """
    def get(self):
        """
        Set the response code to 201 adn return custom headers
        :return:
        """
        return {'task': 'Hello world'}, 201, {'Etag': 'some-opaque-string'}


# api url
api.add_resource(HelloWorld, '/', '/hello')
# api.add_resource(TodoSimple, '/<string:todo_id>')
# api.add_resource(Todo1, '/<int:todo_id>', endpoint='todo_ep')


# argument parsing
parser = reqparse.RequestParser()
parser.add_argument('rate', type=int, help='Rate to charge for this resource')
# return dict
# args = parser.parse_args(strict=True)


TODOS = {
    'todo1': {'task': 'build an API'},
    'todo2': {'task': '?????'},
    'todo3': {'task': 'profit!'}
}


def abort_if_todo_doesnt_exist(todo_id):
    """
    todo_id not in TODOS
    :param todo_id:
    :return:
    """
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exxist.".format(todo_id))


parser = reqparse.RequestParser()
parser.add_argument('task')


class Todo(Resource):
    """
    Todo
    shows a single todo item and lets you delete a todo item
    """
    def get(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        return TODOS[todo_id]

    def delete(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        del TODOS[todo_id]
        return '', 204

    def put(self, todo_id):
        args = parser.parse_args()
        task = {'task': args['task']}
        TODOS[todo_id] = task
        return task, 201


class TodoList(Resource):
    """
    TodoList
    shows a list of all todos, and lets you POST to add new tasks
    """
    def get(self):
        return TODOS

    def post(self):
        args = parser.parse_args()
        todo_id = int(max(TODOS.keys()).lstrip('todo')) + 1
        TODOS[todo_id] = {'task': args['task']}
        return TODOS[todo_id], 201


# actually setup the api resource routing here
api.add_resource(TodoList, '/todos')
api.add_resource(Todo, '/todos/<todo_id>')


if __name__ == '__main__':
    app.run(debug=True)
