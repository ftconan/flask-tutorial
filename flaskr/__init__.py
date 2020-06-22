# coding=utf-8

"""
@author: magician
@date: 2018/9/11
"""
import os

from flask import Flask

from flaskr import db, auth, blog


def create_app(test_config=None):
    """
    create and configure the app
    :param test_config:
    :return:
    """
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr')
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello World!'

    # register the database commands
    from flaskr import db

    # init db
    db.init_app(app)

    # apply the blueprints to the app
    from flaskr import auth, blog

    # register auth blueprint module
    app.register_blueprint(auth.bp)
    app.register_blueprint(blog.bp)

    app.add_url_rule('/', endpoint='index')

    return app
