from db import get_connection
from staging import run_etl


def main():
    with get_connection() as conn:
        run_etl(conn)


if __name__ == "__main__":
    main()
