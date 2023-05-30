import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='service hw4 start script')
    parser.add_argument('command', type=str, choices=['start', 'stop', 'restart'], help='')

    args = parser.parse_args()
    command = args.command

    if command == "start":
        print("start")
    if command == "stop":
        print("stop")
    if command == "restart":
        print("restart")