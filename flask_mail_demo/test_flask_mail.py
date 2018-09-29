import os

from flask import Flask
from flask_mail import Message, Mail

app = Flask(__name__)

# aliyun
app.config['MAIL_SERVER'] = 'smtp.mxhichina.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_SSL'] = True
app.config['MAIL_USERNAME'] = 'xx@xx.net'
app.config['MAIL_PASSWORD'] = 'xxx'
app.config['DEBUG'] = True
mail = Mail(app)

# 创建邮件内容
msg = Message('pdf test',sender='xx@xx.net', recipients=['xx@qq.com'])
msg.body='邮件正文内容'

# 发送邮件，没有包含附件
# with app.app_context():
#     mail.send(msg)


# 发送邮件，包含有附件
with app.app_context():
    with app.open_resource('采购报表.pdf') as f:
        # msg.attach 邮件附件添加
        # msg.attach("文件名", "类型", 读取文件）
        msg.attach('采购报表.pdf','test/pdf',f.read())
        mail.send(msg)

if __name__ == '__main__':
    app.run()
