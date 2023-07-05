import smtplib

server = smtplib.SMTP('email-smtp.us-east-1.amazonaws.com', 587)

server.starttls()

server.login('AKIAT2LA77I325VULSPB', 'BKwoSe8o5DqZwCJ8T+0Ieau66iAxf/qSB1H9JekUls7n')

server.sendmail('krishnan.in@mouritech.com', 'krishna.rupesh@nomupay.com', 'lolol')

print("lol")


