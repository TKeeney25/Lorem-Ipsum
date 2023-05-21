import datetime
import yagmail
from utils import settings


def send_email(recipients, subject, contents, attachments=None):
    if attachments is None:
        attachments = []
    with yagmail.SMTP(settings['from'], settings['gmail_secret']) as yag:
        yag.send(recipients, 'TickerTracker ' + subject, [str(datetime.datetime.now())] + contents, attachments)


def debug_email(exceptions):
    recipients = settings['send_debug']
    subject = 'Exception'
    contents = ['Exception Occurred!']
    for exception in exceptions:
        contents.append(repr(exception))
    attachments = ['./logs/app.log']
    send_email(recipients, subject, contents, attachments)


def start_email():
    recipients = settings['send_start']
    subject = 'Started'
    contents = ['Job Started.']
    send_email(recipients, subject, contents)


def complete_email():
    recipients = settings['send_complete']
    subject = 'Completed'
    contents = ['Job Completed.']
    attachments = ['./data/tickers.csv']
    send_email(recipients, subject, contents, attachments)
