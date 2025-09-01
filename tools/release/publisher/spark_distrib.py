#!/usr/bin/env python
import argparse
import os
import re
import requests
from .remote_manager import Client, ClientBuilder, spark_distrib_remote_dir
from .utils import configure_logger

logger = configure_logger("Spark distrib uploader")

SPARK_BASE_URL = 'https://archive.apache.org/dist/spark'
MAVEN_CONNECT_URL = 'https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12'
VERSION_REGEX = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def _parse_version(version):
    match = VERSION_REGEX.fullmatch(version)
    if match is None:
        msg = f"Version ${version} is in invalid format"
        logger.error(msg)
        raise RuntimeError(msg)
    return [int(match[i + 1]) for i in range(3)]


def spark_download_urls(version):
    minor = _parse_version(version)[1]
    if minor <= 2:
        return [f"{SPARK_BASE_URL}/spark-{version}/spark-{version}-bin-hadoop3.2.tgz"]
    urls = [f"{SPARK_BASE_URL}/spark-{version}/spark-{version}-bin-hadoop3.tgz"]
    if minor >= 4:
        connect_url = f"{MAVEN_CONNECT_URL}/{version}/spark-connect_2.12-{version}.jar"
        urls.append(connect_url)
    return urls


def validate_and_check_version(version):
    '''
    Checks format and existence of tgz Spark distributive with specified version
    :param version:
    :return:
    '''
    maj = _parse_version(version)[0]

    if maj != 3:
        msg = "Spark versions other than 3.X.X are not supported (yet)"
        logger.error(msg)
        raise RuntimeError(msg)

    artifact_urls = spark_download_urls(version)

    for url in artifact_urls:
        response = requests.head(url)

        if response.status_code != 200:
            msg = f"Required artifact for Spark version {version} does not exist at {url}"
            logger.error(msg)
            raise RuntimeError(msg)


def upload_distributive(version, client: Client, ignore_existing: bool, distrib_bytes, filename: str):
    maj, min, patch = _parse_version(version)
    distrib_root = spark_distrib_remote_dir(maj, min, patch)
    distrib_path = f"{distrib_root}/{filename}"
    if client.exists(distrib_path) and not ignore_existing:
        logger.info(f"Spark {version} distributive already exists")
        return
    logger.info(f"Uploading Spark {version} distributive file {filename}")
    if not client.exists(distrib_root):
        client.mkdir(distrib_root)
    client.write_file(distrib_bytes, distrib_path)


def upload_artifacts(version, client: Client, ignore_existing: bool, use_cache=False, cache_path: str = "/tmp"):
    artifact_urls = spark_download_urls(version)

    for url in artifact_urls:
        filename = url.split("/")[-1]

        if use_cache:
            cached_file = f"{cache_path}/{filename}"
            logger.info(f"Cache is enabled. File {cached_file} will be used")
            if not os.path.exists(cached_file):
                logger.info(f"No cached archive found, downloading it from {url}")
                with open(cached_file, 'wb') as f:
                    f.write(requests.get(url).content)
            with open(cached_file, 'rb') as f:
                distrib_bytes = f.read()
        else:
            response = requests.get(url)
            distrib_bytes = response.content
        upload_distributive(version, client, ignore_existing, distrib_bytes, filename)


def main(versions, root, ignore_existing, use_cache, cache_path):
    logger.info(f"{versions} versions of Spark will be deployed")
    for version in versions:
        validate_and_check_version(version)

    client = Client(ClientBuilder(root_path=root))

    for version in versions:
        upload_artifacts(version, client, ignore_existing, use_cache=use_cache, cache_path=cache_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Spark distributive publisher")
    parser.add_argument("--root", default="//home/spark", type=str, help="Root spyt path on YTsaurus cluster")
    parser.add_argument('--ignore-existing', action='store_true',
                        dest='ignore_existing', help='Overwrite cluster files')
    parser.set_defaults(ignore_existing=False)
    parser.add_argument("--use-cache", action='store_true', help='Cache downloaded Spark archives')
    parser.add_argument("--cache-path", default="/tmp", type=str, help="A directory with cached Spark archives")
    parser.add_argument("versions", metavar="version", type=str, nargs='*', help="Spark version formatted as X.X.X")

    args = parser.parse_args()
    main(args.versions, args.root, args.ignore_existing, args.use_cache, args.cache_path)
