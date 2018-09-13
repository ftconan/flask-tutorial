# coding=utf-8

"""
@author: magician
@date: 2018/9/13
"""
import datetime as dt
import uuid
# from collections import OrderedDict

from marshmallow import Schema, fields, pprint, post_load, ValidationError, validates
# from marshmallow import INCLUDE


class User(object):
    """
    user model
    """
    def __init__(self, name, email, **kwargs):
        self.name = name
        self. email = email
        self.created_at = kwargs.get('created_at') if kwargs.get('created_at') else dt.datetime.now()
        # self.created_at = kwargs.get('created_at') or dt.datetime.now()

    def __repr__(self):
        return '<User(name={self.name!r})>'.format(self=self)


class UserSchema(Schema):
    """
    user schema
    """
    name = fields.Str()
    email = fields.Email()
    created_at = fields.DateTime()

    @post_load
    def make_user(self, data):
        return User(**data)


class BandMemberSchema(Schema):
    """
    band member schema
    """
    name = fields.String(required=True)
    email = fields.Email()


def validate_quantity(n):
    """
    validate quantity
    :param n:
    :return:
    """
    if n < 0:
        raise ValidationError('Quantity must be greater than 0.')
    if n > 30:
        raise ValidationError('Quantity must not be greater than 30.')


class ItemSchema(Schema):
    """
    validate integer
    """
    # quantity = fields.Integer(validate=validate_quantity)
    quantity = fields.Integer()

    @validates
    def validate_quantity(self, value):
        """
        validate quantity
        :param value:
        :return:
        """
        if value < 0:
            raise ValidationError('Quantity must be greater than 0.')
        if value > 30:
            raise ValidationError('Quantity must not be greater than 30.')


class ValidateUserSchema(UserSchema):
    """
    This is a contrived example
    You could use marshmallow.validate.Tange instead of an anonymous function here
    """
    age = fields.Number(validate=lambda n: 18 <= n <= 40)


class UserSchema(Schema):
    # name = fields.String(required=True)
    # age = fields.Integer(required=True)
    # name = fields.String()
    email_addr = fields.String(attribute='email')
    date_created = fields.DateTime(attribute='created_at')
    #
    # name = fields.String()
    # email = fields.Email(data_key='emailAddress')

    # class Meta:
    #     unknown = INCLUDE


if __name__ == '__main__':
    # serializing objects(dumping)
    user = User(name='Monty', email='monty@python.org')
    schema = UserSchema()
    result = schema.dump(user)
    pprint(result)

    # json using dumps
    json_user = '{"name": "Monty", "email": "monty@python.org", "created_at": "2014-08-17T14:54:16.049594+00:00"}'
    json_result = schema.dumps(user)
    pprint(json_result)

    # filtering output
    summary_schema = UserSchema(only=('name', 'email'))
    summary_result = summary_schema.dump(user)
    pprint(summary_result)

    # dserializing objects(loading)
    user_data = {
        'created_at': '2014-08-11T05:26:03.869245',
        'email': u'ken@yahoo.com',
        'name': u'Ken'
    }
    schema = UserSchema()
    result = schema.load(user_data)
    pprint(result)

    # deserializing to objects
    new_user_data = {
        'name': 'Ronnie',
        'email': 'ronnie@stones.com'
    }
    schema = UserSchema()
    result = schema.load(new_user_data)
    print(result)

    # handling collections of objects
    user1 = User(name='Mick', email='mick@stones.com')
    user2 = User(name='Keith', email='keith@stones.com')
    users = [user1, user2]
    schema = UserSchema(many=True)
    result = schema.dump(users)
    # pprint(result)
    print(result)

    # validation
    try:
        result = UserSchema().load({'name': 'John', 'email': 'foo'})
    except ValidationError as err:
        print(err.messages)
        valid_data = err.valid_data
        print(valid_data)

    # validating a collection
    user_data = [
        {'email': 'mick@stones.com', 'name': 'Mick'},
        {'email': 'invalid', 'name': 'Invalid'},  # invalid email
        {'email': 'keith@stones.com', 'name': 'Keith'},
        {'email': 'charlie@stones.com'},  # missing "name"
    ]

    try:
        BandMemberSchema(many=True).load(user_data)
    except ValidationError as err:
        print(err.messages)

    # perform additional validation
    in_data = {'name': 'Mick', 'email': 'mick@stones.com', 'age': 71}
    try:
        result = ValidateUserSchema().load(in_data)
    except ValidationError as err:
        print(err.messages)

    # validation function
    in_data = {'quantity': 31}
    try:
        result = ItemSchema().load(in_data)
    except ValidationError as err:
        print(err.messages)

    # partial loading
    result = UserSchema().load({'age': 42}, partial=('name',))
    print(result)

    result = UserSchema().load({'age': 42}, partial=True)
    print(result)

    # schema validate
    errors = UserSchema().validate({'name': 'Ronnie', 'email': 'invalid-email'})
    print(errors)

    # handling unknown fields
    # schema = UserSchema(unknown=INCLUDE)
    # data = {'name': 'Ronnie', 'email': 'invalid-email'}
    # UserSchema().load(data, unknown=INCLUDE)

    # specifying attribute names
    user = User('Keith', email='keith@stones.com')
    ser = UserSchema()
    result = ser.dump(user)
    pprint(result)

    s = UserSchema()
    data = {
        'name': 'Mike',
        'email': 'foo@bar.com'
    }
    result = s.dump(data)
    print(result)

    data = {
        'name': 'Mike',
        'emailAddress': 'foo@bar.com'
    }
    result = s.load(data)
    print(result)

    # refactoring: implicit field creation
    class UserSchema(Schema):
        """
        user schema
        """
        name = fields.Str()
        email = fields.Email()
        created_at = fields.DateTime()
        uppername = fields.Function(lambda  obj: obj.name.upper())

        class Meta:
            fields = ('name', 'email', 'created_at', 'uppername')
            # additional = ('name', 'email', 'created_at', 'uppername')
            ordered = True

    # ordering output
    u = User('Charlie', 'charlie@stones.com')
    schema = UserSchema()
    result = schema.dump(u)
    print(type(result))
    # assert isinstance(result, OrderedDict)
    # marshmallow's pprint function maintains order
    pprint(result, indent=2)

    # read-only and write-only
    # class UserSchema(Schema):
    #     name = fields.Str()
    #     # password is "write-only"
    #     password = fields.Str(load_only=True)
    #     # created_at is "read-only"
    #     created_at = fields.DateTime(dump_only=True)

    # specify default
    class UserSchema(Schema):
        id = fields.UUID(missing=uuid.uuid1)
        birthdate = fields.DateTime(default=dt.datetime(2017, 9, 29))


    UserSchema().load({})
    UserSchema().dump({})
