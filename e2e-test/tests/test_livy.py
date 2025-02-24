import json
import requests
import time


def wait_state_change(url, initial_state):
    state = initial_state
    while state == initial_state:
        time.sleep(2)
        r = requests.get(url).json()
        state = r.get('state', 'unknown')
    return state


def create_session(host):
    r = requests.post(host + '/sessions', data=json.dumps({'kind': 'spark'}))
    session_url = host + r.headers['location']
    state = wait_state_change(session_url, 'starting')
    assert state == 'idle'
    return session_url


def wrap_sql_query(sql_query):
    return '{{' \
           'import tech.ytsaurus.spyt.serializers.GenericRowSerializer;' \
           'val df = spark.sql(\"{0}\");' \
           'println(GenericRowSerializer.dfToYTFormatWithBase64(df, 10).mkString(\"\\n\"))' \
           '}}'.format(sql_query)


def run_code(host, session_url, code):
    r = requests.post(session_url + '/statements', data=json.dumps({'code': code}))
    statement_url = host + r.headers['location']
    state = wait_state_change(statement_url, 'running')
    assert state == 'available'
    output = requests.get(statement_url).json().get('output', {})
    return output


def extract_data(output):
    return output.get('data', {}).get('text/plain', '').strip()


def test_livy_server(yt_client, tmp_dir, livy_server):
    table = f"{tmp_dir}/table"
    yt_client.create("table", table, attributes={"schema": [{"name": "id", "type": "string"}]})
    yt_client.write_table(table, [{'id': '0x123'}, {'id': '0xABACABA'}])

    host = "http://" + livy_server.rest()
    session_url = create_session(host)

    output = run_code(host, session_url, "println(3)")
    assert extract_data(output) == '3'

    output = run_code(host, session_url, wrap_sql_query("select 1"))
    assert extract_data(output) == 'F\nFQAAAAAAAAAKDwoBMRADQIUgSAFSAwiFIBABGAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAA' \
                                   'ABAAAAAAAAAA=='

    output = run_code(host, session_url, wrap_sql_query(f"select * from yt.`ytTable:/{table}`"))
    assert extract_data(output) == 'F\nFgAAAAAAAAAKEAoCaWQQEEAQSABSBBICCBAQARgAAAACAAAAAAAAAAEAAAAAAAAAAAAAAAAAAA' \
                                   'AFAAAAAAAAADB4MTIzAAAAAQAAAAAAAAAAAAAAAAAAAAkAAAAAAAAAMHhBQkFDQUJBAAAAAAAAAA=='


def test_read_non_latin_symbols(yt_client, tmp_dir, livy_server):
    table = f"{tmp_dir}/non_latin_table"
    yt_client.create("table", table, attributes={"schema": [{"name": "id", "type": "int64"},
                                                            {"name": "value", "type": "string"}]})
    yt_client.write_table(table, [{"id": 1, "value": "Номер один"},
                                  {"id": 2, "value": "Номер два"},
                                  {"id": 3, "value": "Что то ещё"}])

    host = "http://" + livy_server.rest()
    session_url = create_session(host)

    query = f"select * from yt.`ytTable:/{table}` WHERE value LIKE 'Номер%' ORDER BY id"
    output = run_code(host, session_url, wrap_sql_query(query))
    # The expected result is the same as in DataFrameSerializerTest::"serialize non-latin symbols in unicode" unit test
    assert extract_data(output) == 'F\nKwAAAAAAAAAKEAoCaWQQA0ADSABSBBICCAMKEwoFdmFsdWUQEEAQSABSBBICCBAQARgAAAAAAA' \
                                   'ACAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAABAAAAAAAAABMAAAAAAAAA0J3QvtC80LXRgCDQvtC0' \
                                   '0LjQvQAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAEQAAAAAAAADQndC+0LzQtdGAINC00L' \
                                   'LQsAAAAAAAAAA='
