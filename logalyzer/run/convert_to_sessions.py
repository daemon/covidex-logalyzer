from pathlib import Path
import argparse

from logalyzer.data import SessionLogParser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('logfile', type=Path)
    args = parser.parse_args()

    log_parser = SessionLogParser(args.logfile)
    for query_session in log_parser.read_sessions():
        print(query_session.json())


if __name__ == '__main__':
    main()
