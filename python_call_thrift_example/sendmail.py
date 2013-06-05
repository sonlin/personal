#!/usr/bin/env python
# -*- coding: utf8 -*-


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
import cgi


def mail(sender, receivers, subject, text, html=None):
    if not html:
        html = ("<html><head></head><body><pre>" + cgi.escape(text) +
            "</pre></body></html>")
    msg = MIMEMultipart('alternative')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = sender
    msg['To'] = ', '.join(receivers)
    part1 = MIMEText(text, 'plain', 'utf-8')
    part2 = MIMEText(html, 'html', 'utf-8')
    msg.attach(part1)
    msg.attach(part2)
    s = smtplib.SMTP('mail.yy.com')
    s.login('webtech_bugadmin@yy.com', 'E6N5EY10n1iyO2K')
    s.sendmail(sender, receivers, msg.as_string())
    s.quit()


def main():
    import sys, argparse
    parser = argparse.ArgumentParser(description='Send mails to one or more'
        ' receivers. Content are read from STDIN.')
    parser.add_argument('sender')
    parser.add_argument('receiver', nargs='+')
    parser.add_argument('-s', '--subject', required=True)
    args = parser.parse_args()
    text = sys.stdin.read()
    return mail(args.sender, args.receiver, args.subject, text)


if __name__ == '__main__':
    main()