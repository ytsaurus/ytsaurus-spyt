import os


def my_function():
    return "MY FUNCTION"


def verbalizer(x):
    host = os.uname().nodename
    return f"Verbalizing {x} on host {host}"
