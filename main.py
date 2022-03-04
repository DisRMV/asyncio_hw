import os
import asyncio
import aiosqlite
import aiosmtplib
from email.message import EmailMessage
from more_itertools import chunked
from typing import Iterable

SERVICE_NAME = os.getenv('SERVICE_NAME', default='Test_adv_service')
SERVICE_EMAIL = os.getenv('SERVICE_EMAIL', default='root@localhost')
NUMBER_OF_CONTACTS = os.getenv('NUMBER_OF_CONTACTS', default=100)
USER_MAIL = os.getenv('USER_MAIL', default='')
PASSWORD = os.getenv('PASSWORD', default='')


async def get_data(person_id):
    async with aiosqlite.connect('contacts.db') as db:
        async with db.execute(f'SELECT first_name, last_name, email FROM contacts WHERE contact_id = {person_id};') as cursor:
            data = await cursor.fetchone()
    return data


async def send_message(first_name, second_name, email):
    message = EmailMessage()
    message['From'] = SERVICE_EMAIL
    message['To'] = email
    message['Subject'] = f'От "{SERVICE_NAME}" с наилучшими пожеланиями!'
    content = f'Уважаемый {first_name} {second_name}!\nСпасибо, что пользуетесь нашим сервисом объявлений.'
    message.set_content(content)

    await aiosmtplib.send(message,
                          hostname="smtp.mail.ru",
                          port=465,
                          username=USER_MAIL,
                          password=PASSWORD,
                          use_tls=True)


async def main(range_id: Iterable[int]):
    for chunk in chunked(range_id, 10):
        task_list = []
        for user_id in chunk:
            data = await get_data(user_id)
            print(data)
            task = send_message(*data)
            task_list.append(task)
        await asyncio.gather(*task_list)


if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(range(1, NUMBER_OF_CONTACTS + 1)))
