# coding=utf-8

"""
@author: magician
@date: 2018/9/13
"""
import datetime as dt
from marshmallow import Schema, fields


class TitleCased(fields.Field):
    """
    title: creating a field class
    """
    def _serialize(self, value, attr, obj):
        if value is None:
            return ''
        return value.title()


class UserSchema(Schema):
    """
    user schema
    """
    name = fields.String()
    email = fields.String()
    created_at = fields.String()
    titlename = TitleCased(attribute='name')
    since_created = fields.Method('get_days_since_created')
    uppername = fields.Function(lambda obj: obj.name.upper())
    balance = fields.Method('get_balance', deserialize='load_balance')

    # Function fields optionally receive context argument
    is_author = fields.Function(lambda user, context: user == context['blog'].author)
    likes_bikes = fields.Method('writes_about_bikes')

    def writes_about_bikes(self, user):
        return 'bicycle' in self.context['blog'].title.lower()

    @staticmethod
    def get_days_since_created(obj):
        return dt.datetime.now().day - obj.created_at.day

    @staticmethod
    def get_balance(obj):
        return obj.income - obj.debt

    @staticmethod
    def load_balance(value):
        return float(value)


class MyDate(fields.Date):
    """
    default error message
    """
    default_error_messages = {
        'invalid': 'Please provide a valid date.'
    }


class MemberSchema(Schema):

    name = fields.Str(
        required=True,
        error_messages={'required': 'Please provide a name.'}
    )


if __name__ == '__main__':
    # method and function field deserialization
    schema = UserSchema()
    result = schema.load({'balance': '100.00'})
    print(result.data['balance'])

    # add context
    schema = UserSchema()
    # user = User('Freddie Mercury', 'fred@queen.com')
    # blog = Blog('Bicycle Blog', author=user)

    # schema.context = {'blog': blog}
    # result = schema.dump(user)
    # result['is_author']  # => True
    # result['likes_bikes']  # => True
