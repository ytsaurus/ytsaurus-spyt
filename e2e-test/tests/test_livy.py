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
           'println(GenericRowSerializer.dfToYTFormatWithBase64(df).mkString(\"\\n\"))' \
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
    assert extract_data(output) == 'NQAAAAAAAAAKMQoBMRADSABaKHsidHlwZV9uYW1lIj0ib3B0aW9uYWwiOyJpdGVtIj0iaW50Nj' \
                                   'QiO30YAAAAAAEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAA'

    output = run_code(host, session_url, wrap_sql_query(f"select * from yt.`ytTable:/{table}`"))
    assert extract_data(output) == 'NwAAAAAAAAAKMwoCaWQQEEgAWil7InR5cGVfbmFtZSI9Im9wdGlvbmFsIjsiaXRlbSI9InN0cm' \
                                   'luZyI7fRgAAAIAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAMHgxMjMAAAABAAAAAAAA' \
                                   'AAAAAAAAAAAACQAAAAAAAAAweEFCQUNBQkEAAAAAAAAA'
