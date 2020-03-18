import socket
import time


class ClientError(Exception):
    pass


class Client:
    def __init__(self, addr, port, timeout=None):
        self.addr = addr
        self.port = port
        self.timeuot = timeout

        try:
            self.sock = socket.create_connection((addr, port), timeout)
            self.sock.settimeout(timeout)
        except socket.error as err:
            raise ClientError("Cannot create connection", err)

    def _sendall(self, data):

        try:
            self.sock.sendall(data)
        except socket.error as err:
            raise ClientError("Error sending data to server", err)

    def _recv(self):

        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.sock.recv(1024)
            except socket.error as err:
                raise ClientError("Error reading data from socket", err)

        return data.decode()

    def put(self, metric, value, timestamp=None):
        timestamp = timestamp or int(time.time())
        self._sendall(f"put {metric} {value} {timestamp}\n".encode())
        in_data = self._recv()

        if in_data == 'ok\n\n':
            return in_data

        raise ClientError('Server returns an error')

    def get(self, metric_value):
        data_dict = {}
        self._sendall("get {}\n".format(metric_value).encode())
        data = self._recv()
        status, content = data.split("\n", 1)
        content = content.strip()
        if status != 'ok':
            raise ClientError('Server returns an error')

        if content == '':
            return data_dict

        try:
            for row in content.splitlines():
                key, value, timestamp = row.split()
                if key not in data_dict:
                    data_dict[key] = []
                data_dict[key].append((int(timestamp), float(value)))

            for i in data_dict.values():
                i.sort(key=lambda x: x[0])
                return data_dict

        except Exception as err:
            raise ClientError('Server returns invalid data', err)

    def close(self):
        try:
            self.sock.close()
        except socket.error as err:
            raise ClientError("Error. Do not close the connection", err)


