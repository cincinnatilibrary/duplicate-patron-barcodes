import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders
from os import path


def send_email(
    smtp_username,
    smtp_password,
    subject, 
    from_addr, 
    to_addr,
    message=None,
    html=None,
    files=None
    # cc_addr=None, 
    # bcc_addr=None, 
):
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = COMMASPACE.join(to_addr)
    # msg['Cc'] = COMMASPACE.join(cc_addr)
    # msg['Bcc'] = COMMASPACE.join(bcc_addr)
    msg['Subject'] = subject

    # if there's a message AND html, send both
    if (message and html):
        # the text version ...
        text_part = MIMEText(message, 'plain')
        msg.attach(text_part)
        # the HTML version ...
        html_part = MIMEText(html, 'html')
        msg.attach(html_part)
        
    elif message:
        msg.attach(MIMEText(message))
    elif html:
        msg.attach(MIMEText(html, 'html'))

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=path.basename(file))  
        msg.attach(part)
        
    smtp = smtplib.SMTP('smtp.mandrillapp.com', 587)
    smtp.login(smtp_username, smtp_password)
    smtp.send_message(msg)
    smtp.close()
    
    
# use like this:
# send_email(
#     subject="Here's your data",
#     message="Please find attached the data you requested.",
#     from_addr="sender@example.com",
#     to_addr=["recipient1@example.com", "recipient2@example.com"],
#     files=["test.csv"],
# )