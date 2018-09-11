# coding=utf-8

"""
@author: magician
@date: 2018/9/11
"""
import pytest
from flask import session, g

from flaskr import create_app
from flaskr.db import get_db


def test_register(client, app):
    """
    test register
    :return:
    """
    assert  client.get('/auth/register').status_code == 200
    response = client.post(
        '/auth/register', data={'username': 'a', 'password': 'a'}
    )
    assert 'http://localhost/auth/login' == response.headers['Location']

    with app.app_context():
        assert get_db().execute("select * from user where username = 'a'").fetchone() is not None


@pytest.mark.parametrize(('username', 'password', 'message'), (
        ('', '', b'Username is required.'),
        ('a', '', b'Password is required.'),
        ('test', 'test', b'already registered')
))
def test_register_validate_input(client, username, password, message):
    """
    test register validate input
    :param client:
    :param username:
    :param password:
    :param message:
    :return:
    """
    response = client.post(
        '/auth/register',
        data={'username': username, 'password': password}
    )
    assert message in response.data


def test_login(client, auth):
    """
    test login
    :param client:
    :param auth:
    :return:
    """
    assert client.get('/auth/login').status_code == 200
    response = auth.login()
    assert response.headers['Location'] == 'http://localhost/'

    with client:
        client.get('/')
        assert  session['user_id'] == 1
        assert  g.user['username'] == 'test'


@pytest.mark.parametrize(('username', 'password', 'message'), (
        ('a', 'test', b'Incorrect username.'),
        ('test', 'a', b'Incorrect password.')
))
def test_login_validate_input(auth, username, password, message):
    """
    test login validate input
    :param auth:
    :param username:
    :param password:
    :param message:
    :return:
    """
    response = auth.login(username, password)
    assert message in response.data


def test_logout(client, auth):
    """
    test logout
    :param client:
    :param auth:
    :return:
    """
    auth.login()

    with client:
        auth.logout()
        assert 'user_id' not in session
