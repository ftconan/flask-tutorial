# coding=utf-8

"""
@author: magician
@date: 2018/9/11
"""
import sqlite3

import click
from flask import current_app, g
from flask.cli import with_appcontext


def get_db():
    """
    get db connection
    :return:
    """
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    """
    close db connection
    :return:
    """
    db = g.pop('db', None)

    if db is not None:
        db.close()


def init_db():
    """
    execute sql
    :return:
    """
    db = get_db()

    with current_app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf-8'))


@click.command('init-db')
@with_appcontext
def init_db_command():
    """
    Clear the existing data adn create new tables.
    :return:
    """
    init_db()
    click.echo('Initialized the database.')


def init_app(app):
    """
    init app
    :param app:
    :return:
    """
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
