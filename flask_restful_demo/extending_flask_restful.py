# coding=utf-8

"""
@author: magician
@date: 2018/9/14
"""
from functools import wraps

import flask_restful
from flask import Flask, make_response
from flask_restful import Api, fields, reqparse
from flask_restful.representations import json


# define custom error messages
errors = {
    'UserAlreadyExistsError': {
        'message': "A user with that username already exists.",
        'status': 409
    },
    'ResourceDoesNotExist': {
        'message': "A resource with that ID no longer exists.",
        'status': 410,
        'extra': "Any extra information you want."
    }
}


app = Flask(__name__)
api = Api(app, errors=errors)


# content negotiation
@api.representation('application/json')
def output_json(data, code, headers=None):
    """
    output json
    :param data:
    :param code:
    :param headers:
    :return:
    """
    resp = make_response(json.dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


# custom fields & inputs
class AllCapsString(fields.Raw):
    def format(self, value):
        return value.upper()


# example usage
# fields = {
#     'name': fields.String,
#     'all_caps_name': AllCapsString(attribute=name)
# }

def odd_number(value, name):
    if value % 2 == 0:
        raise ValueError("The parameter '{}' is not odd. You gave us the value: {}".format(name, value))

    return value


def task_status(value):
    statuses = [u"init", u"in-progress", u"completed"]
    return statuses.index(value)


# response formats
@api.representation('text/csv')
def output_csv(data, code, headers=None):
    pass
    # implement csv output!


# resource method decorators
def basic_authentication():
    return ''


def authenticate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not getattr(func, 'authenticated', True):
            return func(*args, **kwargs)

        acct = basic_authentication()  # custom account lookup function

        if acct:
            return func(*args, **kwargs)

        flask_restful.abort(401)
    return wrapper


class Resource(flask_restful.Resource):
    method_decorators = [authenticate]   # applies to all inherited resources


if __name__ == '__main__':
    # test task_status
    # parser = reqparse.RequestParser()
    # parser.add_argument('OddNumber', type=odd_number)
    # parser.add_argument('Status', type=task_status)
    # args = parser.parse_args()
    pass
