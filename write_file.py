import json
import os


def readjs(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data


def writejs(path, data, indent=2):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
        return True


def readfile(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = f.readlines()
        return data


def writeline(path, data, append='a'):
    f = open(path, append, encoding='utf-8')
    f.write(data)
    f.close()

def writedata(path, data, append='a'):
    f = open(path, append, encoding='utf-8')
    f.writelines(data)
    f.close()