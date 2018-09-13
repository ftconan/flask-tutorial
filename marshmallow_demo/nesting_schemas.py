# coding=utf-8

"""
@author: magician
@date: 2018/9/13
"""
import datetime as dt

from marshmallow import Schema, fields, pprint


class User(object):
    """
    User
    """
    def __init__(self, name, email):
        self.name = name
        self.email = email
        self.created_at = dt.datetime.now()
        self.friends = []
        self.employer = None


class Blog(object):
    def __init__(self, title, author):
        self.title = title
        self.author = author


class UserSchema(Schema):
    """
    user schema
    """
    name = fields.String()
    email = fields.Email()
    # created_at = fields.DateTime()
    # friends = fields.Nested('self', only='name', many=True)
    friends = fields.Nested('self', many=True)
    # use the 'exclude' argument to avoid infinite recursion
    employer = fields.Nested('self', exclude=('employer', ), default=None)


class BlogSchema(Schema):
    """
    blog schema
    """
    title = fields.String()
    # nested field
    author = fields.Nested(UserSchema)


class BlogSchema2(Schema):
    """
    blog schema2
    specifying which fields to nest
    """
    title = fields.String()
    # nested field
    author = fields.Nested(UserSchema, only=['email'])


class SiteSchema(Schema):
    """
    nest using dot delimiters
    """
    blog = fields.Nested(BlogSchema2)


class AuthorSchema(Schema):
    """
    Make sure to use the 'only' or 'exclude' params
    to avoid infinite recursion
    """
    books = fields.Nested('BookSchema', many=True, exclude=('author', ))

    class Meta:
        fields = ('id', 'name', 'books')


class BookSchema(Schema):
    author = fields.Nested(AuthorSchema, only=('id', 'name'))

    class Meta:
        fields = ('id', 'title', 'author')


if __name__ == '__main__':
    user = User(name='Monty', email='monty@python.org')
    blog = Blog(title='Something Completely Different', author=user)
    result = BlogSchema().dump(blog)
    pprint(result)

    # specifying which fields to nest
    schema = BlogSchema2()
    result = schema.dump(blog)
    pprint(result)

    # nest using dot delimiters
    schema = SiteSchema(only=['blog.author.email'])
    site = {
        'blog': {
            'author': {'email': u'monty@python.org'}
        }
    }
    result = schema.dump(site)
    pprint(result)

    # only value deserialized
    # new_user = {
    #     'name': 'Steve',
    #     'email': 'steve@example.com',
    #     'friends': ['Mike', 'Joe']
    # }
    # serialized_data = UserSchema().dump(new_user)
    # pprint(serialized_data)
    # deserialized_data = UserSchema().load(result)
    # pprint(deserialized_data)

    # two-way nesting
    # author = Author(name='William Faulkner')
    # book = Book(title='As I Lay Dying', author=author)
    # book_result = BookSchema().dump(book)
    # pprint(book_result, indent=2)
    # {
    #   "id": 124,
    #   "title": "As I Lay Dying",
    #   "author": {
    #     "id": 8,
    #     "name": "William Faulkner"
    #   }
    # }

    # author_result = AuthorSchema().dump(author)
    # pprint(author_result, indent=2)
    # {
    #   "id": 8,
    #   "name": "William Faulkner",
    #   "books": [
    #     {
    #       "id": 124,
    #       "title": "As I Lay Dying"
    #     }
    #   ]
    # }

    # nesting a schema within itself
    user = User('Steve', 'steve@example.com')
    user.friends.append(User('Mike', 'mike@example.com'))
    user.friends.append(User('Joe', 'joe@example.com'))
    user.employer = User('Dirk', 'dirk@example.com')
    result = UserSchema().dump(user)
    pprint(result, indent=2)
