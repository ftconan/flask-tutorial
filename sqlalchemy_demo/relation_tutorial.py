# coding=utf-8

"""
@author: magician
@file: relation_tutorial.py
@date: 2018/11/7
"""
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Sequence, and_, or_, text, func, ForeignKey, exists, \
    Text, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased, relationship
from sqlalchemy.orm.strategy_options import selectinload, joinedload, contains_eager

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

    class NewUser(Base):
        __tablename__ = 'new_users'
        # Firebird and Oracle need sequence
        id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
        name = Column(String(50))
        fullname = Column(String(50))
        password = Column(String(50))

        def __repr__(self):
            return "<User(name='%s', fullname='%s', password='%s')>" % (self.name, self.fullname, self.password)


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

    # label() SQL:AS
    for row in session.query(User.name.label('name_label')).all():
        print(row.name_label)

    # aliased()
    user_alias = aliased(User, name='user_alias')
    for row in session.query(user_alias, user_alias.name).all():
        print(row.user_alias)

    # SQL: LIMIT:2 OFFSET:1
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
    # ilike(不区分大小写的LIKE,有些不支持）
    print(session.query(User).filter(User.name.ilike('%ed%')))
    # in
    print(session.query(User).filter(User.name.in_(['ed', 'wendy', 'jack'])))
    # not in
    print(session.query(User).filter(~User.name.in_(['ed', 'wendy', 'jack'])))
    # is null
    print(session.query(User).filter(User.name == None))
    # alternatively, if pep8/linters are a concern
    print(session.query(User).filter(User.name.is_(None)))
    # is not null
    print(session.query(User).filter(User.name != None))
    # alternatively, if pep8/linters are a concern
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
    class Address(Base):
        __tablename__ = 'addresses'
        id = Column(Integer, primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey('users.id'))
        user = relationship('User', back_populates='addresses')

        def __repr__(self):
            return "<Address(email_address='%s')>" % self.email_address

    User.addresses = relationship('Address', order_by=Address.id, back_populates='user')

    Base.metadata.create_all(engine)

    # working with related objects
    jack = User(name='jack', fullname='Jack Bean', password='gjffdd')
    print(jack.addresses)
    jack.addresses = [
        Address(email_address='jack@google.com'),
        Address(email_address='j25@yahoo.com')
    ]
    print(jack.addresses[1])
    print(jack.addresses[1].user)

    session.add(jack)
    session.commit()

    jack = session.query(User).filter_by(name='jack').one()
    print(jack)
    print(jack.addresses)

    # querying with joins
    for u, a in session.query(User, Address).filter(User.id == Address.user_id).filter(
            Address.email_address == 'jack@google.com').all():
        print(u)
        print(a)

    print(session.query(User).join(Address).filter(Address.email_address == 'jack@google.com').all())
    # query.join(Address, User.id == Address.user_id)
    # query.join(User.addresses)
    # query.join(Address, User.addresses)
    # query.join('addresses')
    # query.outerjoin(User.addresses)
    # query = session.query(User, Address).select_from(Address).join(User)

    # using aliases
    adalias1 = aliased(Address)
    adalias2 = aliased(Address)
    for username, email1, email2 in session.query(User.name, adalias1.email_address, adalias2.email_address).join(
            adalias1, User.addresses).join(adalias2, User.addresses).filter(
            adalias1.email_address == 'jack@google.com').filter(adalias2.email_address == 'j25@yahoo.com'):
        print(username, email1, email2)

    # subquery
    stmt = session.query(Address.user_id, func.count('*').label('address_count')).group_by(Address.user_id).subquery()
    for u, count in session.query(User, stmt.c.address_count).outerjoin(stmt, User.id == stmt.c.user_id).order_by(
            User.id):
        print(u, count)

    stmt = session.query(Address).filter(Address.email_address != 'j25@yahoo.com').subquery()
    adalias = aliased(Address, stmt)
    for user, address in session.query(User, adalias).join(adalias, User.addresses):
        print(user)
        print(address)

    # using exists
    stmt = exists().where(Address.user_id == User.id)
    for name in session.query(User.name).filter(stmt):
        print(name)

    for name in session.query(User.name).filter(User.addresses.any()):
        print(name)

    for name in session.query(User.name).filter(Address.email_address.like('%google%')):
        print(name)

    session.query(Address).filter(~Address.user.has(User.name == 'jack')).all()

    # common relationship operators
    # someuser = session.query(User.name == 'jack')

    # query.filter(Address.user != someuser)

    query.filter(Address.user == None)

    someaddress = session.query(Address).filter(~Address.user.has(User.name == 'jack')).first()

    query.filter(User.addresses.contains(someaddress))

    query.filter(User.addresses.any(Address.email_address == 'bar'))
    # also takes keyword arguments:
    query.filter(User.addresses.any(email_address='bar'))

    query.filter(Address.user.has(name='ed'))

    # session.query(Address).with_parent(someuser, 'addresses')

    # eager loading
    # 1.selectin load
    # jack = session.query(User).options(selectinload(User.addresses)).filter_by(name='jack').one()
    # print(jack)
    # print(jack.addresses)

    # 2.joined load
    # jack = session.query(User).options(joinedload(User.addresses)).filter_by(name='jack').one()
    # print(jack)
    # print(jack.addresses)

    # 3.explicit join eagerload
    # jacks_addresses = session.query(Address).join(Address.user).filter(User.name == 'jack').options(
    #     contains_eager(Address.user)).all()
    # print(jacks_addresses)
    # print(jacks_addresses[0].user)

    # deleting
    session.delete(jack)
    print(session.query(User).filter_by(name='jack').count())

    print(session.query(Address).filter(Address.email_address.in_(['jack@google.com',
                                                                   'j25@yahoo.com'])).count())

    # configure delete/delete-orphan cascade
    session.close()


    # class Address(Base):
    #     """
    #     Address
    #     """
    #     __table_name__ = 'addresses'
    #     id = Column(Integer, primary_key=True)
    #     email_address = Column(String, nullable=False)
    #     user_id = Column(Integer, ForeignKey('user.id'))
    #     user = relationship('User', back_populates='addresses')
    #
    # def __repr__(self):
    #     return "<Address(email_address='%s')>" % self.email_address
    #
    # load Jack by primary key
    # jack = session.query(User).get(5)
    # # remove on Address(lazy load fires off)
    # del jack.addresses[1]
    # only one address remains
    # session.query(Address).filter(Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])).count()

    # building a many to many relationship
    # association table
    post_keywords = Table(
        'post_keywords',
        Base.metadata,
        Column('post_id', ForeignKey('posts.id'), primary_key=True),
        Column('keyword_id', ForeignKey('keywords.id'), primary_key=True)
    )

    class BlogPost(Base):
        """
        BlogPost
        """
        __table_name__ = 'posts'
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('users.id'))
        headline = Column(Integer, nullable=False)
        body = Column(Text)

        # many to many BlogPost<-->Keyword
        keywords = relationship('Keyword', secondary=post_keywords, back_populates='posts')

        def __init__(self, headline, body, author):
            self.author = author
            self.headline = headline
            self.body = body

        def __repr__(self):
            return "BlogPost(%r, %r, %r)" % (self.headline, self.body, self.author)

    class Keyword(Base):
        """
        Keyword
        """
        id = Column(Integer, primary_key=True)
        keyword = Column(String(50), nullable=False, unique=True)
        posts = relationship('BlogPost', secondary=post_keywords, back_populates='keywords')

        def __init__(self, keyword):
            self.keyword = keyword

    BlogPost.author = relationship(User, back_populates='posts')
    User.posts = relationship(BlogPost, back_populates='author', lazy='dynamic')
    Base.metadata.create_all(engine)

    wendy = session.query(User).filter_by(name='wendy').filter_by(name='wendy').one()
    post = BlogPost("Wendy's Blog Post", "This is a test", wendy)
    session.add(post)

    post.keywords.append(Keyword('wendy'))
    post.keywords.append(Keyword('firstpost'))

    print(session.query(BlogPost).filter(BlogPost.keywords.any(keyword='firstpost')).all())

    print(session.query(BlogPost).filter(BlogPost.author == wendy).filter(BlogPost.keywords.any(keyword='firstpost')).all())
    print(wendy.posts.filter(BlogPost.keywords.any(keyword='firstpost')).all())
