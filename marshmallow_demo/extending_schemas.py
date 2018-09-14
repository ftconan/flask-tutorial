# coding=utf-8

"""
@author: magician
@date: 2018/9/14
"""
import logging

from flask import request
from marshmallow import Schema, fields, pre_load, post_dump, post_load, ValidationError, validates_schema, SchemaOpts


# class UserSchema(Schema):
#     """
#     user schema
#     """
#     name = fields.Str()
#     slug = fields.Str()
#
#     @pre_load
#     def slugify_name(self, in_data):
#         """
#         before load: transform slug
#         :param in_data:
#         :return:
#         """
#         in_data['slug'] = in_data['slug'].lower().strip().replace(' ', '-')
#         return in_data


class User(object):
    """
    User
    """
    def __init__(self, name, email):
        self.name = name
        self.email = email

    def __repr__(self):
        return '<User(name={0})>'.format(self.name)


class BaseSchema(Schema):
    """
    custom options
    """
    __envelope__ = {
        'single': None,
        'many': None
    }
    __model__ = User

    def get_envelop_key(self, many):
        """
        helper to get the envelope key.
        :param many:
        :return:
        """
        key = self.__envelope__['many'] if many else self.__envelope__['single']
        assert key is not None, 'Envelope key undefined'
        return key

    @pre_load(pass_many=True)
    def unwrap_envelop(self, data, many):
        key = self.get_envelop_key(many)
        return data.data[key] if not isinstance(data, dict) else data

    @post_dump(pass_many=True)
    def wrap_with_envelope(self, data, many):
        key = self.get_envelop_key(many)
        return {key: data}

    @post_load
    def make_object(self, data):
        return self.__model__(**data)


class UserSchema(BaseSchema):
    __envelope__ = {
        'single': 'user',
        'many': 'users'
    }
    __model__ = User
    name = fields.Str()
    email = fields.Email()

    def hanlde_error(self, exc ,data):
        """
        Log adn raise our custom exception when (de(serialization fails
        :param exc:
        :param data:
        :return:
        """
        logging.error(exc.messages)
        raise AppError('An error occurred with input: {0}'.format(data))


class BandSchema(Schema):
    name = fields.Str()

    @pre_load
    def unwrap_envelope(self, data):
        if 'data' not in data:
            raise ValidationError('Input data must have a "data" key.', '_preprocessing')
        return data['data']


class AppError(Exception):
    pass


class NumberSchema(Schema):
    """
    number schema
    """
    field_a = fields.Integer()
    field_b = fields.Integer()

    @validates_schema
    def validate_numbers(self, data):
        """
        validate numbers
        :param data:
        :return:
        """
        if data['field_b'] >= data['field_a']:
            raise ValidationError('field_a must be greater than field_b')


class MySchema(Schema):
    """
    my schema
    """
    foo = fields.Int()
    bar = fields.Int()

    @post_load(pass_original=True)
    def add_baz_to_bar(self, data, original_data):
        """
        add baz to bar
        :param data:
        :param original_data:
        :return:
        """
        baz = original_data.get('baz')
        if baz:
            data['bar'] = data['bar'] + baz
        return data


class UserDictSchema(Schema):
    """
    user dict schema
    """
    name = fields.Str()
    email = fields.Email()

    # If we know we're only serializing dictionaries, we can
    # use dict.get for all input objects
    def get_attribute(self, obj, key, default):
        return obj.get(key, default)


class NamespaceOpts(SchemaOpts):
    """
    Same as the default class Meta options, but adds "name" and
    "plural_name" options for enveloping
    """
    def __init__(self, meta, **kwargs):
        SchemaOpts.__init__(self, meta, **kwargs)
        self.name = getattr(meta, 'name', None)
        self.plural_name = getattr(meta, 'plural_name', self.name)


class NamespacedSchema(Schema):
    OPTIONS_CLASS = NamespaceOpts

    @pre_load(pass_many=True)
    def unwrap_envelope(self, data, many):
        key = self.opts.plural_name if many else self.opts.name
        return data[key]

    @post_dump(pass_many=True)
    def wrap_with_envelope(self, data, many):
        key = self.opts.plural_name if many else self.opts.name
        return {key: data}


class MemberSchema(NamespacedSchema):
    """
    new user schema
    """
    name = fields.String()
    email = fields.Email()

    class Meta:
        name = 'user'
        plural_name = 'users'


if __name__ == '__main__':
    # pre_processing adn post-processing methods
    # schema = UserSchema()
    # result = schema.load({'name': 'Steve', 'slug': 'Steve Loria '})
    # print(result.data['slug'])

    # envelope(pass_many=True)
    user_schema = UserSchema()
    user = User('Mick', email='mick@stones.org')
    user_data = user_schema.dump(user)
    print(user_data.data)

    users = [
        User('Keith', email='keith@stones.org'),
        User('Charlie', email='charlie@stones.org')
    ]
    users_data = user_schema.dump(users, many=True)
    print(users_data.data)

    user_objs = user_schema.load(users_data, many=True)
    print(user_objs.data)

    # raising errors in pre-/post-processor methods
    sch = BandSchema()
    try:
        sch.load({'name': 'The Band'})
    except ValidationError as err:
        print(err.messages)

    # handling errors
    schema = UserSchema()
    print(schema.load({'name': 'test', 'email': 'invalid-email'}))

    # schema-level validation
    schema = NumberSchema()
    try:
        schema.load({'field_a': 2, 'field_b': 1})
    except ValidationError as err:
        print(err.messages['field_a'])

    # using original input data
    schema = MySchema()
    origin_result = schema.load({'foo': 1, 'bar': 2, 'baz': 3})
    print(origin_result.data)

    # SchemaOpts(custom 'class Meta' options)
    ser = MemberSchema()
    user = User('Keith', email='keith@stones.com')
    result = ser.dump(user)
    print(result.data)

    # using context
    schema = MemberSchema()
    # Make current HTTP request available to
    # custom fields, schema methods, schema validators, etc.
    schema.context['request'] = request
    result = schema.dump(user)
    print(result.data)
