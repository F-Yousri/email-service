import os
import asyncio
import nest_asyncio
from fastapi import BackgroundTasks
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from dotenv import load_dotenv
from confluent_kafka import Consumer
from configparser import ConfigParser
from time import sleep

nest_asyncio.apply()
load_dotenv('.env')

class Envs:
    MAIL_USERNAME = os.getenv('MAIL_USERNAME')
    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD')
    MAIL_FROM = os.getenv('MAIL_FROM')
    MAIL_PORT = int(os.getenv('MAIL_PORT'))
    MAIL_SERVER = os.getenv('MAIL_SERVER')
    MAIL_FROM_NAME = os.getenv('MAIN_FROM_NAME')

conf = ConnectionConfig(
    MAIL_USERNAME=Envs.MAIL_USERNAME,
    MAIL_PASSWORD=Envs.MAIL_PASSWORD,
    MAIL_FROM=Envs.MAIL_FROM,
    MAIL_PORT=Envs.MAIL_PORT,
    MAIL_SERVER=Envs.MAIL_SERVER,
    MAIL_FROM_NAME=Envs.MAIL_FROM_NAME,
    MAIL_TLS=False,
    MAIL_SSL=False,
    USE_CREDENTIALS=False,
    TEMPLATE_FOLDER='templates',
)

# Parse the kafka configuration.
config_parser = ConfigParser()
config_parser.read('kafka-config.ini')
config = dict(config_parser['default'])
config.update(config_parser['consumer'])
# Create Consumer instance
consumer = Consumer(config)
# Subscribe to topic
topic = "emails"
consumer.subscribe([topic])


async def send_email_async(subject: str, email_to: str, body: str):
    message = MessageSchema(
        subject=subject,
        recipients=[email_to],
        body=body,
        subtype='html',
    )
    
    fm = FastMail(conf)
    await fm.send_message(message, template_name='email.html')

def send_email_background(background_tasks: BackgroundTasks, subject: str, email_to: str, body: dict):
    message = MessageSchema(
        subject=subject,
        recipients=[email_to],
        body=body,
        subtype='html',
    )
    fm = FastMail(conf)
    background_tasks.add_task(
       fm.send_message, message, template_name='email.html')

try:
    while True:
        print("Listening")
        # read single message at a time
        msg = consumer.poll(0)

        if msg is None:
            sleep(5)
            continue
        if msg.error():
            print("Error reading message : {}".format(msg.error()))
            continue

        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')
        print(value)
        asyncio.run(send_email_async("new email", "someone@somewhere.com", "sdfsdfsd"))
except Exception as ex:
    print("Kafka Exception : {}", ex)

finally:
    print("closing consumer")
    consumer.close()