import os


def foo():

    print(os.environ["SHELL"])


if __name__ == "__main__":
    foo()
