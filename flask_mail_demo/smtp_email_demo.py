import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

smtpserver = "smtp.qq.com"
smtpport = 465
from_mail = "xx@qq.com"
to_mail = ["xx@qq.com"]
password = "161616161616161616"  # 16位授权码


if __name__ == '__main__':
    try:
        smtp = smtplib.SMTP_SSL(smtpserver, smtpport)
        smtp.login(from_mail, password)

        body = "<h3>hi, the attachment is the test report of this test, please check it in time.</h3>"

        msg = MIMEMultipart()
        msgtext = MIMEText(body, "html", "utf-8")
        msg.attach(msgtext)
        smtp.sendmail(from_mail, to_mail, msg.as_string())
    except Exception as e:
        print(e.message)
    finally:
        smtp.quit()
