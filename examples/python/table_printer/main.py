import argparse
import spyt


def run_job(yt_proxy: str, table_path: str):
    with spyt.direct_spark_session(yt_proxy) as spark:
        df = spark.read.yt(table_path)
        df.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('proxy', help="YT proxy address", required=True)
    parser.add_argument('table_path', help="Table to print", required=True)
    args, _ = parser.parse_known_args()
    run_job(args.proxy, args.table_path)


if __name__ == '__main__':
    main()
