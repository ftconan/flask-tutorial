# coding=utf-8
"""
@author: conan
@date: 2018/9/8
"""
from flask import Flask, request, current_app
from flask_script import Manager, Shell
from flask_mail import Mail, Message
from threading import Thread
import os

app = Flask(__name__)

app.config['DEBUG'] = True

app.config['MAIL_SERVER'] = 'smtp.qq.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME', 'xx@qq.com')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD', '1616161616161616')

manager = Manager(app)
mail = Mail(app)


# 异步发送邮件
def send_async_email(app, msg):
    with app.app_context():
        mail.send(msg)


def send_email(to, subject,  **kwargs):
    app = current_app._get_current_object()
    msg = Message(subject, sender='xx@qq.com', recipients=[to])
    msg.body = 'test'
    msg.html = '<b>test</b>'

    thr = ''
    try:
        thr = Thread(target=send_async_email, args=[app, msg])
        thr.start()
    except Exception as e:
        print(e)
    return thr


@app.route('/')
def index():
    # msg = Message(subject='Email test by flask-email', sender="xx@qq.com", recipients=['xx@qq.com'])
    # msg.body = 'test demo'
    # msg.html = '<b>测试Flask发送邮件<b>'
    #
    # try:
    #     thread = Thread(target=send_async_email, args=[app, msg])
    #     thread.start()
    # except Exception as e:
    #     print(e)

    # mail.send(msg)

    send_email('xx@qq.com', 'test demo')

    return '<h1>邮件发送成功</h1>'


if __name__ == '__main__':
    manager.run()
