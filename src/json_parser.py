import json
import sys


if __name__ == '__main__':
    args = sys.argv[1:]
    key_string = str(args[0])
    keys = key_string.split(".")[1:]
    input_string = sys.stdin.read()
    input_json = json.loads(input_string)

    for key in keys:
        input_json = input_json[key]

    print(str(input_json))