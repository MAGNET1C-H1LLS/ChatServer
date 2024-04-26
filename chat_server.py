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
await_kick_user: list = []

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
                    client_id, client_username = get_auth_client(json_message['username'],
                                                                 json_message['password'])
                    if not client_id or client_id in online_clients.keys():
                        raise websockets.exceptions.ConnectionClosedError(None, None)

                    is_admin = check_is_admin(client_id)

                    await websocket.send(f"{client_id} {client_username} {1 if is_admin else 0}")

                    is_authorized = True
                    online_clients[client_id] = websocket

                    await init_client(client_id)
            else:
                if is_user_is_kick(websocket):
                    raise websockets.exceptions.ConnectionClosedError(None, None)

                if is_admin:
                    if 'remove' in json_message:
                        await remove_message(message)
                    elif 'ban' in json_message:
                        ban_user(message)
                    elif 'statistic' in json_message:
                        statistic_user(message)
                    else:
                        await notify_users(processing_message(message))
                else:
                    await notify_users(processing_message(message))

    finally:
        print('connection close')
        client_id = 0
        for key, value in online_clients.items():
            if value == websocket:
                client_id = key

        if client_id:
            del online_clients[client_id]

            await send_online_status(processing_online_status(client_id, False))


def is_user_is_kick(websocket: websockets) -> bool:
    global await_kick_user
    global online_clients

    id_this_user = 0

    if len(await_kick_user):
        for item in online_clients.items():
            if websocket == item[1]:
                id_this_user = item[0]

    return id_this_user in await_kick_user


def ban_user(message):
    global connection_bd
    global cursor_bd
    global await_kick_user

    message = json.loads(message)
    print(message)

    if not is_user_is_kick(message['idUser']):
        cursor_bd.execute('''INSERT INTO Ban_users ("ID_user", "Start_ban_period", "End_ban_period") 
                                   VALUES (?, ?, ?);''',
                          (message['idUser'],
                           datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
                           message['endPeriod']))
        connection_bd.commit()

        await_kick_user.append(message['idUser'])


def statistic_user(message): ...  # этот метод отправки статистики по пользователю


def check_is_admin(user_id: int) -> bool:
    global cursor_bd
    return bool(len(cursor_bd.execute(f'SELECT * FROM Admins WHERE ID_user=?', (user_id,)).fetchall()))


async def init_client(client_id: int) -> None:
    global online_clients

    await online_clients[client_id].send('S')
    await send_all_users(online_clients[client_id])
    await send_history_messages(online_clients[client_id], create_history_message())
    await send_online_status(processing_online_status(client_id, True))
    await online_clients[client_id].send('R')


async def notify_users(message: str) -> None:
    global online_clients

    if online_clients:
        await asyncio.gather(*[client.send('0' + f'{message}') for client in online_clients.values()])


def get_auth_client(name: str, password: str) -> tuple:
    cursor_bd.execute('SELECT * FROM Users WHERE Name=? AND Password=?', (name, password))
    res_query = cursor_bd.fetchall()

    if len(res_query):
        id_user = int(res_query[0][0])

        print(is_user_is_ban(id_user))
        if not is_user_is_ban(id_user):
            return id_user, res_query[0][1]
    return None, None


def is_user_is_ban(id_user: int) -> bool:
    global cursor_bd

    cursor_bd.execute('''
        SELECT * 
        FROM Ban_users
        WHERE ID_user = ? AND 
        ? BETWEEN Start_ban_period AND End_ban_period;''',
                      (id_user,
                       datetime.now().strftime("%Y.%m.%d %H:%M:%S")))

    return bool(len(cursor_bd.fetchall()))


async def send_history_messages(current_client: websockets, messages: list) -> None:
    if online_clients:
        await asyncio.gather(*[current_client.send('0' + f'{message}') for message in messages])


async def send_all_users(current_client: websockets):
    if online_clients:
        await asyncio.gather(*[current_client.send('2' + json.dumps(client)) for client in all_client])


async def send_online_status(session: dict) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('1' + json.dumps(session)) for client in online_clients.values()])


async def remove_message(message: str) -> None:
    id_message = json.loads(message)['idMessage']

    await send_delete_message(message)
    delete_message_on_server(id_message)
    delete_message_in_BD(id_message)


async def send_delete_message(message: str) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('3' + f'{message}') for client in online_clients.values()])


def delete_message_on_server(id_message: int) -> None:
    global cursor_bd
    global loaded_messages
    global new_messages

    for i in range(len(loaded_messages)):
        if json.loads(loaded_messages[i])['ID'] == id_message:
            del loaded_messages[i]

    for i in range(len(new_messages)):
        if json.loads(new_messages[i])['ID'] == id_message:
            del new_messages[i]


def delete_message_in_BD(id_message: int) -> None:
    global connection_bd
    global cursor_bd

    cursor_bd.execute('DELETE FROM Messages WHERE ID_message = ?', (id_message,))

    connection_bd.commit()


async def send_delete_user(message: str) -> None:
    if online_clients:
        await asyncio.gather(*[client.send('4' + f'{message}') for client in online_clients.values()])


def delete_user_on_server(id_user: int) -> None:
    global online_clients
    global all_client

    if id_user in online_clients.keys():
        del online_clients[id_user]

    for i in range(len(all_client)):
        if id_user == all_client[i]['ID']:
            del all_client[i]


def delete_user_in_BD(id_user: int) -> None:
    global connection_bd
    global cursor_bd

    cursor_bd.execute('DELETE FROM Users WHERE ID = ?', (id_user,))

    connection_bd.commit()


async def save_in_bd() -> None:
    global new_messages
    global new_sessions

    global connection_bd
    global cursor_bd

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
