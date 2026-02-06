from socket import *

HOST = '127.0.0.1'
PORT = 8080

class Client:
    def run(self):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect((HOST, PORT)) # connect to server (block until accepted)
        s.send(b"Hello, world") # send same data
        data = s.recv(1024) # receive the response
        print(data) # print what you received
        s.send(b"") # tell the server to close
        s.close() # close the connection

if __name__ == "__main__":
    client = Client()
    client.run()