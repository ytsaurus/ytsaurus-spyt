import logging
import time
import unittest


def assert_items_equal(result, expected):
    case = unittest.TestCase()
    case.maxDiff = None
    case.assertCountEqual(result, expected)


def wait_for_operation(yt_client, operation_id):
    if operation_id is not None:
        while True:
            current_state = yt_client.get_operation_state(operation_id)
            logging.info(f"Operation: {operation_id}, State: {current_state}")
            if current_state.is_finished():
                return current_state
            time.sleep(1)
