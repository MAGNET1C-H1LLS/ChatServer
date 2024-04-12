import asyncio
import websockets
import json

async def hello():
    uri = "ws://localhost:8765"  # Замените на URL вашего WebSocket сервера
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({
            'username': 'user1',
            'password': 'user1'
        }))
        async for message in websocket:
            print(f"< {message}")

# Запуск нового экземпляра событийного цикла asyncio
asyncio.run(hello())

# Или если событийный цикл уже запущен в вашем контексте, вы можете использовать следующий подход:
# loop = asyncio.get_event_loop()
# loop.run_until_complete(hello())
