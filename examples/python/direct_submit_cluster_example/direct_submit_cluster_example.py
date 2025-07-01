from spyt.submit import direct_submit_binary
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--proxy', help="YT proxy address", required=True)
    parser.add_argument('--num-executors', help="Number of Spark executors", required=True)
    parser.add_argument('--spyt-version', help="SPYT version to use on the cluster", required=True)
    parser.add_argument('--input', help="Path to a random table that must have an integer id column", required=True)
    parser.add_argument('--output', help="Path to an output table that will be overwritten in the job", required=True)
    args = parser.parse_args()
    spark_base_args = ["--executor-cores", "4", "--executor-memory", "16G"]

    direct_submit_binary(args.proxy, args.num_executors, args.spyt_version,
                         driver_entry_point='spyt_direct_submit_cluster_example.job',
                         spark_base_args=spark_base_args,
                         job_args=[args.input, args.output])


if __name__ == '__main__':
    main()
