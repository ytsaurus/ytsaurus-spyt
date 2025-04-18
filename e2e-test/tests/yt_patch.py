import argparse
from common.cypress import patch_conf
import logging
from yt.wrapper import YtClient


def main(proxy, python_path, java_home):
    client = YtClient(proxy=proxy, config={"token": "token"})
    patch_conf(client, python_path, java_home)
    # Patches for standalone cluster discovery
    client.set("//home/spark/conf/global/environment/YT_NETWORK_PROJECT_ID", "0")
    client.set("//home/spark/conf/global/environment/YT_IP_ADDRESS_DEFAULT", "localhost")
    logging.info(f"SPYT cluster conf patched. Python path: {python_path}, Java home: {java_home}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Patch for cypress conf")
    parser.add_argument('--proxy', type=str, default="localhost:8000", help='YTsaurus proxy')
    parser.add_argument("--python-path", type=str, default="python3", help="Path to python interpreter")
    parser.add_argument("--java-home", type=str, default="/opt/jdk11", help="Path to java home")
    args = parser.parse_args()

    main(args.proxy, args.python_path, args.java_home)
