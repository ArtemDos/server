import socket
import select


def run_server(host, port):
    def reading_data(data, data_base):
        if not data.endswith(b"\n"):
            answer = 'error\nwrong command\n\n'.encode()
            return answer

        data = data.decode('utf-8')
        try:
            status, payload = data.split(' ', 1)
        except:
            answer = 'error\nwrong command\n\n'.encode()
            return answer
        if status == 'put':
            flag = 1
            tmp = ()
            try:
                key, value, timespend = payload.split(' ')
            except:
                answer = 'error\nwrong command\n\n'.encode()
                return answer
            try:
                value = float(value)
                timespend = int(timespend)
            except:
                answer = 'error\nwrong command\n\n'.encode()
                return answer
            if data_base.get(key, None):
                for elem in data_base[key]:
                    if elem[0] == timespend:
                        tmp = elem
                        flag = 0
                        break
                if flag == 1:
                    data_base[key].append((timespend, value))
                else:
                    data_base[key].remove(tmp)
                    data_base[key].append((timespend, value))
            else:
                data_base[key] = [(timespend, value)]
            answer = 'ok\n\n'.encode()
            return answer
        if status == 'get':
            if len(payload.split(' ')) != 1:
                answer = 'error\nwrong command\n\n'.encode()
                return answer
            else:
                key, robber = payload.split('\n', 1)
            answer = ('ok\n' + data_base_func(data_base, key)).encode()
            return answer
        answer = 'error\nwrong command\n\n'.encode()
        return answer

    def data_base_func(data_base, key):
        print(key)
        string = ''
        if key == '*':
            for keys in data_base.keys():
                for elem in data_base.get(keys):
                    string = string + keys + ' ' + str(elem[1]) + ' ' + str(elem[0]) + '\n'
        else:
            if key not in data_base:
                string = string + '\n'
            else:
                for elem in data_base.get(key):
                    string = string + key + ' ' + str(elem[1]) + ' ' + str(elem[0]) + '\n'
        string = string + '\n'
        return string

    sock = socket.socket()
    sock.bind((host, port))
    sock.listen()
    sock.setblocking(False)
    inputs = [sock]  # сокеты, которые будем читать
    outputs = []  # сокеты, в которые надо писать
    messages = {}  # здесь будем хранить сообщения для сокетов
    data_base = {}

    while True:

        reads, send, excepts = select.select(inputs, outputs, inputs)
        for conn in reads:
            if conn == sock:
                new_conn, client_addr = conn.accept()
                inputs.append(new_conn)

            else:
                data = conn.recv(1024)
                if data:
                    data = reading_data(data, data_base)
                    if messages.get(conn, None):
                        messages[conn].append(data)
                    else:
                        messages[conn] = [data]

                    if conn not in outputs:
                        outputs.append(conn)

        # список SEND - сокеты, готовые принять сообщение
        for conn in send:
            msg = messages.get(conn, None)
            if len(msg):
                temp = msg.pop(0)
                conn.send(temp)
                print(data_base)
            else:
                outputs.remove(conn)

        # список EXCEPTS - сокеты, в которых произошла ошибка
        for conn in excepts:
            inputs.remove(conn)
            if conn in outputs:
                outputs.remove(conn)
            conn.close()
            del messages[conn]
