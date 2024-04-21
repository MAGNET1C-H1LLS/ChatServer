import asyncio
import json
import sqlite3

import websockets
import psycopg2

from datetime import datetime


LIMIT_HISTORY_MESSAGE: int = 100
SAVE_PERIOD: int = 60
# DATABASE: str = "postgresql://test:123@localhost:5432/test"

connection_bd: sqlite3.Connection = sqlite3.connect('mydatabase.db')
cursor_bd: sqlite3.Cursor = connection_bd.cursor()

online_clients: dict = {}
all_client: list = []
new_sessions: list = []
sessions_keys: list = []

loaded_messages: list = []
new_messages: list = []

last_message_id = 0


async def handle_client(websocket: websockets, path: str) -> None:
    global all_client
    global online_clients

    is_authorized = False
    is_admin = False

    try:
        async for message in websocket:
            json_message = json.loads(message)

            if not is_authorized:
                if 'username' in json_message and 'password' in json_message:
                    client_id, client_username = get_auth_client(json_message['username'], # нужно переделать, чтобы забаненного не пускало
                                                                 json_message['password']) # также добавить в бд таблицу с банном
                    if not client_id or client_id in online_clients.keys():
                        raise websockets.exceptions.ConnectionClosedError(None, None)

                    is_admin = await check_is_admin(client_id)

                    await websocket.send(f"{client_id} {client_username} {1 if is_admin else 0}")

                    is_authorized = True
                    online_clients[client_id] = websocket

                    await init_client(client_id)
            else:
                if is_admin:
                    if 'remove' in json_message:
                        await delete_data(message)
                    # нужно чтобы сессия пользователя заканчивалась как-то
                    elif 'ban' in json_message:
                        await ban_user(message)
                    elif 'statistic' in json_message:
                        await statistic_user(message)
                    else:
                        await notify_users(processing_message(message))
                else:
                    await notify_users(processing_message(message))

    except websockets.exceptions.ConnectionClosedError:
        ...

    finally:
        client_id = 0
        for key, value in online_clients.items():
            if value == websocket:
                client_id = key

        if client_id:
            del online_clients[client_id]

            await send_online_status(processing_online_status(client_id, False))


async def ban_user(message): ... # этот метод временного бана пользователя
# нужно чтобы сессия пользователя заканчивалась как-то


async def statistic_user(message): ... # этот метод отправки статистики по пользователю


async def check_is_admin(user_id: int) -> bool:
    return bool(len(cursor_bd.execute(f'SELECT * FROM Admins WHERE ID_user=?', (user_id,)).fetchall()))


async def init_client(client_id: int) -> None:
    await online_clients[client_id].send('S')
    await send_all_users(online_clients[client_id])
    await send_history_messages(online_clients[client_id], create_history_message())
    await send_online_status(processing_online_status(client_id, True))
    await online_clients[client_id].send('R')


async def notify_users(message: str) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('0' + f'{message}') for client in online_clients.values()])


def get_auth_client(name: str, password: str) -> tuple: # нужно переделать, чтобы забаненного не пускало
    # также добавить в бд таблицу с банном
    cursor_bd.execute('SELECT * FROM Users WHERE Name=? AND Password=?', (name, password))
    res_query = cursor_bd.fetchall()

    if len(res_query):
        return int(res_query[0][0]), res_query[0][1]
    return None, None


async def send_history_messages(current_client: websockets, messages: list) -> None:
    if online_clients:
        await asyncio.gather(*[current_client.send('0' + f'{message}') for message in messages])


async def send_all_users(current_client: websockets):
    if online_clients:
        await asyncio.gather(*[current_client.send('2' + json.dumps(client)) for client in all_client])


async def send_online_status(session: dict) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('1' + json.dumps(session)) for client in online_clients.values()])


async def delete_data(message: str) -> None:
    if 'idMessage' in message:
        id_message = json.loads(message)['idMessage']

        await asyncio.gather(send_delete_message(message),
                             delete_message_in_BD(id_message))

    elif 'idUser' in message:
        id_user = json.loads(message)['idUser']

        await asyncio.gather(send_delete_user(message),
                             delete_user_in_BD(id_user))


async def send_delete_message(message: str) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('3' + f'{message}') for client in online_clients.values()])


async def delete_message_in_BD(id_message: int) -> None: ... # этот метод удаления сообщения из бд и отправки другим пользователям сообщения об удалении


async def send_delete_user(message: str) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('4' + f'{message}') for client in online_clients.values()])


async def delete_user_in_BD(id_user: int) -> None: ... # этот метод удаления пользователя из бд, если время его на пожизненно банят
# нужно чтобы сессия пользователя заканчивалась как-то


async def save_in_bd() -> None:
    global new_messages
    global new_sessions

    while True:
        await asyncio.sleep(SAVE_PERIOD)

        if len(new_messages):
            for message in new_messages:
                message = json.loads(message)
                cursor_bd.execute("INSERT INTO Messages (ID_user, Date, Message) VALUES (?, ?, ?)",
                                  (message['OwnerID'], message['Date'], message['Message']))

        if len(new_sessions):
            for session in new_sessions:
                cursor_bd.execute("INSERT INTO Chat_sessions (ID_user, Online_status, Date) VALUES (?, ?, ?)",
                                  (session['ID'], session['OnlineStatus'], session['Date']))
        connection_bd.commit()
        print('Save')
        update_loaded_messages()
        new_sessions.clear()


def update_loaded_messages() -> None:
    global new_messages
    global loaded_messages

    if len(loaded_messages + new_messages) > LIMIT_HISTORY_MESSAGE:
        loaded_messages = (loaded_messages + new_messages)[-LIMIT_HISTORY_MESSAGE:]
    else:
        loaded_messages += new_messages

    new_messages.clear()


def create_history_message() -> list:
    global loaded_messages
    global new_messages

    return (loaded_messages + new_messages)[-LIMIT_HISTORY_MESSAGE:]


def load_users_from_BD() -> None:
    global cursor_bd
    global all_client

    cursor_bd.execute(
         '''SELECT DISTINCT Users.ID, Users.Name, Chat_sessions.Online_status
            FROM Users
            LEFT JOIN (
            SELECT ID_user, MAX(Date) as MaxDate
            FROM Chat_sessions
            GROUP BY ID_user
            ) as Chat_sessions_max ON Users.ID = Chat_sessions_max.ID_user
            LEFT JOIN Chat_sessions ON Chat_sessions_max.ID_user = Chat_sessions.ID_user AND
            Chat_sessions_max.MaxDate = Chat_sessions.Date'''
        )
    for row in cursor_bd.fetchall():
        all_client.append((
            {
                'ID': int(row[0]),
                'Name': str(row[1]),
                'OnlineStatus': True if str(row[2]) == 'True' else False
            }
        ))


def load_messages_from_BD() -> None:
    global cursor_bd
    global loaded_messages
    global last_message_id

    cursor_bd.execute(f"SELECT * FROM Messages ORDER BY ID_message DESC LIMIT {LIMIT_HISTORY_MESSAGE};")
    for row in cursor_bd.fetchall()[::-1]:
        loaded_messages.append(json.dumps(
            {
                'ID': int(row[0]),
                'OwnerID': int(row[1]),
                'Date': str(row[2]),
                'Message': str(row[3])
            }
        ))

        if row[0] > last_message_id:
            last_message_id = row[0]


def processing_message(message: str) -> str:
    global new_messages
    global last_message_id

    message = json.loads(message)
    message['Date'] = datetime.now().strftime("%Y.%m.%d %H:%M:%S")

    last_message_id += 1
    message['ID'] = last_message_id

    message = json.dumps(message)

    new_messages.append(message)

    return message


def processing_online_status(client_id: int, online_status: bool) -> dict:
    global new_sessions

    session = {
            'ID': client_id,
            'OnlineStatus': online_status,
            'Date': datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        }
    new_sessions.append(session)

    change_online_status_client(client_id, online_status)

    return session


def change_online_status_client(client_id: int, online_status: bool) -> None:
    global all_client

    for i in range(len(all_client)):
        if int(all_client[i]['ID']) == int(client_id):
            all_client[i]['OnlineStatus'] = online_status


def main() -> None:
    load_users_from_BD()
    load_messages_from_BD()

    start_server = websockets.serve(handle_client, "localhost", 8765)

    asyncio.get_event_loop().run_until_complete(asyncio.gather(start_server, save_in_bd()))
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
