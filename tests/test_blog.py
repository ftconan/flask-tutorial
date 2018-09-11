# coding=utf-8

"""
@author: magician
@date: 2018/9/11
"""
import pytest
from flaskr.db import get_db


def test_index(client, auth):
    """
    test index
    :param client:
    :param auth:
    :return:
    """
    response = client.get('/')
    assert b"Log In" in response.data
    assert b"Register" in response.data

    auth.login()
    response = client.get('/')
    assert b'Log Out' in response.data
    assert b'test title' in response.data
    assert b'by test on 2018-01-01' in response.data
    assert b'test\nbody' in response.data
    assert b'href="/1/update"' in response.data


@pytest.mark.parametrize('path', (
        '/create',
        '/1/update',
        '/1/delete'
))
def test_login_required(client, path):
    """
    test login required
    :param client:
    :param path:
    :return:
    """
    response = client.post(path)
    assert response.headers['Location'] == 'http://localhost/auth/login'


def test_author_required(app, client, auth):
    """
    change the post author to another user
    :param app:
    :param client:
    :param auth:
    :return:
    """
    with app.app_context():
        db = get_db()
        db.execute('UPDATE post SET author_id = 2 WHERE id = 1')
        db.commit()

    auth.login()
    # current user can't modify other user's post
    assert client.post('/1/update').status_code == 403
    assert client.post('/1/delete').status_code == 403
    # current user doesn't see edit link
    assert b'href="/1/update"' not in client.get('/').data


@pytest.mark.parametrize('path', (
    '/2/update',
    '/2/delete'
))
def test_exists_required(client, auth, path):
    """
    test exists required
    :param client:
    :param auth:
    :param path:
    :return:
    """
    auth.login()
    assert client.post(path).status_code == 404


def test_create(client, auth, app):
    """
    test create blog
    :param client:
    :param auth:
    :param app:
    :return:
    """
    auth.login()
    assert client.get('/create').status_code == 200
    client.post('/create', data={'title': 'created', 'body': ''})

    with app.app_context():
        db = get_db()
        count = db.execute('SELECT COUNT(id) FROM post').fetchone()[0]
        assert count == 2


def test_update(client, auth, app):
    """
    test blog update
    :param client:
    :param auth:
    :param app:
    :return:
    """
    auth.login()
    assert client.get('/1/update').status_code == 200
    client.post('/1/update', data={'title': 'updated', 'body': ''})

    with app.app_context():
        db = get_db()
        post = db.execute('SELECT * FROM post WHERE id = 1').fetchone()
        assert  post['title'] == 'updated'


@pytest.mark.parametrize('path', (
    '/create',
    '/1/update',
))
def test_create_update_validate(client, auth, path):
    """
    test blog delete
    :param client:
    :param auth:
    :param path:
    :return:
    """
    auth.login()
    response = client.post(path, data={'title': '', 'body': ''})
    assert b'Title is required.' in response.data


def test_delete(client, auth, app):
    """
    test blog delete
    :param client:
    :param auth:
    :param app:
    :return:
    """
    auth.login()
    response = client.post('/1/delete')
    assert response.headers['Location'] == 'http://localhost/'

    with app.app_context():
        db = get_db()
        post = db.execute('SELECT * FROM post WHERE id = 1').fetchone()
        assert post is None
