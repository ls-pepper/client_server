import asyncio
import argparse


class ClientServerProtocol(asyncio.Protocol):
    storage = []

    class Storage:
        def __init__(self, key, value, timestamp):
            self.key = str(key)
            self.value = float(value)
            self.timestamp = int(timestamp)

        def __str__(self):
            return f'{self.key} {self.value} {self.timestamp}\n'

    @staticmethod
    def process_data(data_str):
        s = 'ok\n'
        method, content = data_str.split(maxsplit=1)
        if method == 'put':
            key, value, timestamp = [i for i in content.split()]
            if not ClientServerProtocol.storage:
                ClientServerProtocol.storage.append(ClientServerProtocol.Storage(key, value, timestamp))
            else:
                for i in ClientServerProtocol.storage:
                    if i.key == key and i.timestamp == int(timestamp):
                        i.value = value
                        return s + '\n'
                ClientServerProtocol.storage.append(ClientServerProtocol.Storage(key, value, timestamp))
            return s + '\n'
        elif method == 'get':
            key, value = [i for i in content.split()]
            if key == '*':
                for i in ClientServerProtocol.storage:
                    s = f'{s}{i}'
                return s + '\n'
            else:
                for i in ClientServerProtocol.storage:
                    if i.key == key:
                        s = f'{s}{i}'
                return s + '\n'
        else:
            raise ValueError

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        try:
            resp = self.process_data(data.decode())
            self.transport.write(resp.encode())
        except (ValueError, IndexError):
            resp = 'error\nwrong command\n\n'
            self.transport.write(resp.encode())


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)

    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='host')
    parser.add_argument('--port', dest='port')

    args = parser.parse_args()
    run_server(args.host, args.port)
