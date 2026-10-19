import socket

def hello(arg=None):
    print(f'Hello DRAGON from {socket.gethostname()}!!', flush=True)


if __name__ == "__main__":
    hello()
