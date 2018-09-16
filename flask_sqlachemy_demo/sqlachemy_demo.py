#coding=utf-8
"""
@author: conan
@date: 2018/9/15
"""
from datetime import datetime

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import joinedload

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)


class User(db.Model):
    """
    user model
    """
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    def __repr__(self):
        return '<User %r>' % self.username


# class Foo(db.Model):
#     """
#     foo
#     """
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         pass


class Post(db.Model):
    """
    post model
    """
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(80), nullable=False)
    body = db.Column(db.Text, nullable=False)
    pub_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow())

    category_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=False)
    category = db.relationship('Category', backref=db.backref('posts', lazy=True))

    def __repr__(self):
        return '<Post %r>' % self.title


class Category(db.Model):
    """
    category model
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return '<Category %r>' % self.name


# one-to-many relationships
class Person(db.Model):
    """
    person model
    parent table
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    addresses = db.relationship('Address', lazy='select', backref=db.backref('person', lazy='joined'))


class Address(db.Model):
    """
    address model
    children table
    """
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), nullable=False)
    person_id = db.Column(db.Integer, db.ForeignKey('perosn.id'), nullable=False)


# many-to-many relationships
tags = db.Table('tags',
                db.Column('tag_id', db.Integer, db.ForeignKey('tag.id'), primary_key=True),
                db.Column('page_id', db.Integer, db.ForeignKey('page.id'), primary_key=True)
                )


class Page(db.Model):
    """
    page model
    """
    id = db.Column(db.Integer, primary_key=True)
    tags = db.relationship('Tag', secondary=tags, lazy='subquery', backref=db.backref('pages', lazy=True))


class Tag(db.Model):
    """
    tag model
    """
    id = db.Column(db.Integer, primary_key=True)


if __name__ == '__main__':
    db.create_all()
    admin = User(username='admin', email='admin@example.com')
    guest = User(username='guest', email='guest@example.com')
    db.session.add(admin)
    db.session.add(guest)
    db.session.commit()
    print(User.query.all())
    print(User.query.filter_by(username='admin').first())

    py = Category(name='Python')
    Post(title='Hello Python!', body='Python is pretty cool', category=py)
    p = Post(title='Snakes', body='Sssssss')
    py.posts.append(p)
    db.session.add(py)
    db.session.commit()
    print(py.posts)

    query = Category.query.options(joinedload('posts'))
    for category in query:
        print(category, category.posts)
    print(Post.query.with_parent(py).filter(Post.title != 'Snakes').all())
