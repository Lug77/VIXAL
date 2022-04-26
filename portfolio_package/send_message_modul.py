from dataclasses import dataclass
import smtplib
from smtplib import SMTPDataError
import os
# Добавляем необходимые подклассы - MIME-типы
import mimetypes  # Импорт класса для обработки неизвестных MIME-типов, базирующихся на расширении файла
from email import encoders  # Импортируем энкодер
from email.mime.base import MIMEBase  # Общий тип
from email.mime.text import MIMEText  # Текст/HTML
from email.mime.image import MIMEImage  # Изображения
from email.mime.audio import MIMEAudio  # Аудио
from email.mime.multipart import MIMEMultipart  # Многокомпонентный объект


@dataclass()
class SendMessage:
    addr_from: str = ''  # Отправитель
    password: str = ''  # Пароль ящика отправителя
    server_name: str = ''  # Имя сервера
    port_name: int = 0  # Порт сервера
    name_class = 'SendMessage::'

    def send_email(self, addr_to: str, msg_subj: str, msg_text: str):
        # если не определены адрес или пароль, выходим
        if self.addr_from == 'your_email' or self.password == 'password':
            return
        name_func = 'send_email: '
        msg = MIMEMultipart()  # Создаем сообщение
        msg['From'] = self.addr_from  # Адресат
        msg['To'] = addr_to  # Получатель
        msg['Subject'] = msg_subj  # Тема сообщения

        body = msg_text  # Текст сообщения
        msg.attach(MIMEText(body, 'plain'))  # Добавляем в сообщение текст

        try:
            server = smtplib.SMTP_SSL(self.server_name, self.port_name)  # Создаем объект SMTP (Yandex)
            # server = smtplib.SMTP(self.server_name, self.port_name)  # Создаем объект SMTP (Google)
            # server.starttls()                           # Начинаем шифрованный обмен по TLS (Для Yandex вроде не надо)
            # server.set_debuglevel(True)    # Включаем режим отладки, если не нужен - можно закомментировать
            server.login(self.addr_from, self.password)  # Получаем доступ
            server.send_message(msg)  # Отправляем сообщение
            server.quit()  # Выходим
        except SMTPDataError:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' SMTPDataError')
        except smtplib.SMTPServerDisconnected:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' SMTPServerDisconnected')
        except smtplib.SMTPAuthenticationError:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' SMTPAuthenticationError')
        except TimeoutError:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' TimeoutError')
        except:
            print("Отправка сообщения через : server_name", self.server_name, " type ", type(self.server_name))
            print("Отправка сообщения через : port_name", self.port_name, " type ", type(self.port_name))
            print("Отправка сообщения через : password", self.password, " type ", type(self.password))
            print("Отправка сообщения через : addr_from", self.addr_from, " type ", type(self.addr_from))
            print("На адрес : addr_to", addr_to, " type ", type(addr_to))
            print(self.name_class + name_func + 'Unknown error')
            print('Возможно кирилица в имени компьютера')

    def send_email_html(self, addr_to: str, msg_subj: str, msg_text: str, html: str):
        # если не определены адрес или пароль, выходим
        if self.addr_from == 'your_email' or self.password == 'password':
            return
        name_func = 'send_email: '
        msg = MIMEMultipart()  # Создаем сообщение
        msg['From'] = self.addr_from  # Адресат
        msg['To'] = addr_to  # Получатель
        msg['Subject'] = msg_subj  # Тема сообщения

        body = msg_text  # Текст сообщения
        msg.attach(MIMEText(body, 'plain'))  # Добавляем в сообщение текст
        msg.attach(MIMEText(html, 'html', 'utf-8'))

        try:
            server = smtplib.SMTP_SSL(self.server_name, self.port_name)  # Создаем объект SMTP (Yandex)
            # server = smtplib.SMTP(self.server_name, self.port_name)  # Создаем объект SMTP (Google)
            # server.starttls()                           # Начинаем шифрованный обмен по TLS (Для Yandex вроде не надо)
            # server.set_debuglevel(True)    # Включаем режим отладки, если не нужен - можно закомментировать
            server.login(self.addr_from, self.password)  # Получаем доступ
            server.send_message(msg)  # Отправляем сообщение
            server.quit()  # Выходим
        except SMTPDataError:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' SMTPDataError')
        except smtplib.SMTPServerDisconnected:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' SMTPServerDisconnected')
        except smtplib.SMTPAuthenticationError:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' SMTPAuthenticationError')
        except TimeoutError:
            print("Не удалось отправить сообщение от: ", self.addr_from, " на ", addr_to, ' TimeoutError')
        except:
            print("Отправка сообщения через : server_name", self.server_name, " type ", type(self.server_name))
            print("Отправка сообщения через : port_name", self.port_name, " type ", type(self.port_name))
            print("Отправка сообщения через : password", self.password, " type ", type(self.password))
            print("Отправка сообщения через : addr_from", self.addr_from, " type ", type(self.addr_from))
            print("На адрес : addr_to", addr_to, " type ", type(addr_to))
            print(self.name_class + name_func + 'Unknown error')
            print('Возможно кирилица в имени компьютера')
