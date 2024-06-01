from spyt.utils import format_memory, parse_memory


def test_parse_memory():
    assert parse_memory(128) == 128
    assert parse_memory("128") == 128
    assert parse_memory("256") == 256
    assert parse_memory("256b") == 256
    assert parse_memory("256B") == 256
    assert parse_memory("128k") == 128 * 1024
    assert parse_memory("256k") == 256 * 1024
    assert parse_memory("256K") == 256 * 1024
    assert parse_memory("128kb") == 128 * 1024
    assert parse_memory("256kb") == 256 * 1024
    assert parse_memory("256Kb") == 256 * 1024
    assert parse_memory("256KB") == 256 * 1024
    assert parse_memory("256m") == 256 * 1024 * 1024
    assert parse_memory("256M") == 256 * 1024 * 1024
    assert parse_memory("256mb") == 256 * 1024 * 1024
    assert parse_memory("256Mb") == 256 * 1024 * 1024
    assert parse_memory("256MB") == 256 * 1024 * 1024
    assert parse_memory("256g") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256G") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256gb") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256Gb") == 256 * 1024 * 1024 * 1024
    assert parse_memory("256GB") == 256 * 1024 * 1024 * 1024


def test_format_memory():
    assert format_memory(128) == "128B"
    assert format_memory(256) == "256B"
    assert format_memory(256 * 1024) == "256K"
    assert format_memory(128 * 1024) == "128K"
    assert format_memory(256 * 1024 * 1024) == "256M"
    assert format_memory(256 * 1024 * 1024 * 1024) == "256G"
