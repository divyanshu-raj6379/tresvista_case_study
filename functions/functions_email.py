import smtplib
import os
from email.message import EmailMessage
from configs.settings import settings

SENDER_ = os.environ.get("SENDER_EMAIL")
PWD_ = os.environ.get("SENDER_PWD")

class SendEmail:
    def __init__(self, RECEIVER, FILES):
        self.msg = EmailMessage()
        self.msg['Subject'] = "Successful Data Ingestion"
        self.msg['From'] = SENDER_
        self.msg['To'] = RECEIVER
        self.msg.set_content(
            f'''
            Hi,
            Kindly note that model KPI data has been successfully ingested today.
            PFA the pipeline log and the data written to the DB in a .csv file
            for your reference.
            
            Best Regards,
            Divyanshu''')
        self.files = FILES

    def send_email(self):
        for file in self.files:
            with open(file, 'rb') as f:
                file_data = f.read()
                file_name = (f.name).split('/')[1]
            self.msg.add_attachment(file_data,maintype='application',
                                subtype='octet-stream',filename=file_name)
        
        with smtplib.SMTP_SSL('smtp.mail.yahoo.com',465) as smtp:
            settings.logger.info("Trying to login...")
            try:
                smtp.login(SENDER_, PWD_)
                settings.logger.info("Logged in!")
                settings.logger.info("Sending email...")
                try:
                    smtp.send_message(self.msg)
                    settings.logger.info("Email sent!")
                except Exception as e:
                    settings.logger.error("Unable to send email!")
            except Exception as e:
                settings.logger.error("Unable to Log in!")