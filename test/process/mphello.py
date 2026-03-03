import dragon
import multiprocessing as mp


def f():
    print("Hello World")


def main():
    p = mp.Process(target=f, args=())
    p.start()
    p.join()


if __name__ == "__main__":
    main()
