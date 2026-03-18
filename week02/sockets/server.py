from socket import *

HOST = "127.0.0.1"
PORT = 8080

class Server:
    def run(self):
        s = socket(AF_INET, SOCK_STREAM)
        try:
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen(1)
            conn, addr = s.accept()
            print(f"Connected by {addr}")
            try:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    conn.sendall(data + b"*")
            finally:
                conn.close()
        finally:
            s.close()

if __name__ == "__main__":
    Server().run()
