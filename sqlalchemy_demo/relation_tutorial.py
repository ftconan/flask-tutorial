# coding=utf-8

"""
@author: magician
@file: relation_tutorial
@date: 2018/11/7
"""
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Sequence, and_, or_, text, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased


if __name__ == '__main__':
    # version check
    print(sqlalchemy.__version__)

    # connecting (echo: True: logging False: stop logging)
    engine = create_engine('sqlite:///:memory:', echo=False)

    # declare a mapping
    Base = declarative_base()


    class User(Base):
        __tablename__ = 'users'

        id = Column(Integer, primary_key=True)
        # id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
        name = Column(String)
        fullname = Column(String)
        password = Column(String)

        def __repr__(self):
            return "<User(name='%s', fullname='%s', password='%s')>" % (self.name, self.fullname, self.password)

    # create a schema
    print(User.__tablename__)
    Base.metadata.create_all(engine)

    # create an instance of the mapped class
    ed_user = User(name='ed', fullname='Ed jones', password='edspassword')
    print(ed_user.name)
    print(ed_user.password)
    print(str(ed_user.id))

    # creating session
    Session = sessionmaker(bind=engine)
    # Session.configure(bind=engine)
    session = Session()

    # adding and updating objects
    ed_user = User(name='ed', fullname='Ed Jones', password='edspassword')
    session.add(ed_user)
    our_user = session.query(User).filter_by(name='ed').first()
    print(our_user)

    print(ed_user is our_user)

    session.add_all([
        User(name='wendy', fullname='Wendy Williams', password='foobar'),
        User(name='mary', fullname='Mary Contrary', password='xxg527'),
        User(name='fred', fullname='Fred Flinstone', password='blah')])

    ed_user.password = 'f8s7ccs'
    print(session.dirty)
    print(session.new)

    session.commit()
    # print(ed_user.id)

    # rolling back
    ed_user.name = 'Edwardo'
    fake_user = User(name='fakeuser', fullname='Invaild', password='12345')
    session.add(fake_user)
    session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all()
    session.rollback()
    print(ed_user.name)
    print(fake_user in session)
    session.query(User).filter(User.name.in_(['ed', 'fakeuser'])).all()

    # querying
    for instance in session.query(User).order_by(User.id):
        print(instance.name, instance.fullname)

    for name, fullname in session.query(User.name, User.fullname):
        print(name, fullname)

    for row in session.query(User, User.name).all():
        print(row.User, row.name)

    for row in session.query(User.name.label('name_label')).all():
        print(row.name_label)

    user_alias = aliased(User, name='user_alias')
    for row in session.query(user_alias, user_alias.name).all():
        print(row.user_alias)

    for u in session.query(User).order_by(User.id)[1:3]:
        print(u)

    for name, in session.query(User.name).filter(User.fullname == 'Ed Jones'):
        print(name)

    for user in session.query(User).filter(User.name == 'ed').filter(User.fullname == 'Ed jones'):
        print(user)

    # common filter operators
    # equals
    print(session.query(User).filter(User.name == 'ed'))
    # not equals
    print(session.query(User).filter(User.name != 'ed'))
    # like
    print(session.query(User).filter(User.name.like('%ed%')))
    # ilike
    print(session.query(User).filter(User.name.ilike('%ed%')))
    # in
    print(session.query(User).filter(User.name.in_(['ed', 'wendy', 'jack'])))
    # not in
    print(session.query(User).filter(~User.name.in_(['ed', 'wendy', 'jack'])))
    # is null
    print(session.query(User).filter(User.name == None))
    print(session.query(User).filter(User.name.is_(None)))
    # is not null
    print(session.query(User).filter(User.name != None))
    print(session.query(User).filter(User.name.isnot(None)))
    # and
    print(session.query(User).filter(and_(User.name == 'ed', User.fullname == 'Ed Jones')))
    print(session.query(User).filter(User.name == 'ed', User.fullname == 'Ed Jones'))
    print(session.query(User).filter(User.name == 'ed').filter(User.fullname == 'Ed Jones'))
    # or
    print(session.query(User).filter(or_(User.name == 'ed', User.name == 'wendy')))
    # match
    print(session.query(User).filter(User.name.match('wendy')))

    # returning lists and scalars
    query = session.query(User).filter(User.name.like('%ed')).order_by(User.id)
    # all
    print(query.all())
    # first
    print(query.first())
    # one
    try:
        user = query.one()
        print(user)
        new_user = query.filter(User.id == 99).one()
        print(new_user)

        # scalar
        new_query = session.query(User.id).filter(User.name == 'ed').order_by(User.id)
        print(query.scalar())
    except Exception as e:
        print(str(e))

    # using textual SQL
    for user in session.query(User).filter(text('id<224')).order_by(text('id')).all():
        print(user.name)

    print(session.query(User).filter(text('id<:value and name=:name')).params(value=224, name='fred').order_by(
        User.id).one())

    print(session.query(User).from_statement(text('SELECT * FROM users WHERE name=:name')).params(name='ed').all())

    stmt = text('SELECT name, id, fullname, password FROM users WHERE name=:name')
    stmt = stmt.columns(User.name, User.id, User.fullname, User.password)
    print(session.query(User).from_statement(stmt).params(name='ed').all())

    stmt = text('SELECT name, id FROM users WHERE name=:name')
    stmt = stmt.columns(User.name, User.id)
    print(session.query(User.id, User.name).from_statement(stmt).params(name='ed').all())

    # counting
    print(session.query(User).filter(User.name.like('%ed')).count())

    print(session.query(func.count(User.name), User.name).group_by(User.name).all())

    try:
        print(session.query(func.count('*')).select_from(User).scalar())
        print(session.query(func.count(User.id)).scalar())
    except Exception as e:
        print(e)

    # building a relationship
    pass