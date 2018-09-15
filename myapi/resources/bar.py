# coding=utf-8
"""
@author: magician
@date: 2018/9/15
"""
from flask_restful import fields, marshal_with, reqparse, Resource


def valid_email(email_str):
    return ''


def email(email_str):
    """Return email_str if valid, raise an exception in other case."""
    if valid_email(email_str):
        return email_str
    else:
        raise ValueError('{} is not a valid email'.format(email_str))

post_parser = reqparse.RequestParser()
post_parser.add_argument(
    'username', dest='username',
    location='form', required=True,
    help='The user\'s username',
)
post_parser.add_argument(
    'email', dest='email',
    type=email, location='form',
    required=True, help='The user\'s email',
)
post_parser.add_argument(
    'user_priority', dest='user_priority',
    type=int, location='form',
    default=1, choices=range(5), help='The user\'s priority',
)

user_fields = {
    'id': fields.Integer,
    'username': fields.String,
    'email': fields.String,
    'user_priority': fields.Integer,
    'custom_greeting': fields.FormattedString('Hey there {username}!'),
    'date_created': fields.DateTime,
    'date_updated': fields.DateTime,
    'links': fields.Nested({
        'friends': fields.Url('user_friends'),
        'posts': fields.Url('user_posts'),
    }),
}


def create_user(username, email, user_priority):
    return ''


def fetch_user(id):
    return ''


class User(Resource):

    @marshal_with(user_fields)
    def post(self):
        args = post_parser.parse_args()
        user = create_user(args.username, args.email, args.user_priority)
        return user

    @marshal_with(user_fields)
    def get(self, id):
        args = post_parser.parse_args()
        user = fetch_user(id)
        return user
