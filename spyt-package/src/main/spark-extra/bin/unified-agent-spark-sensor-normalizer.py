#! /usr/bin/env python3
import json
import sys
import traceback


def extract_spark_component(sensor):
    component = None
    if '_driver_' in sensor:
        component = 'driver'
    elif '_executor_' in sensor:
        component = 'executor'
    return component


def extract_app_id_and_clean_sensor(sensor, component):
    sensor_parts = sensor.split('_')
    spark_application_id = sensor_parts[1]
    length_to_delete = 3 if component == 'driver' else 4
    del sensor_parts[1:length_to_delete]
    clean_sensor = "_".join(sensor_parts)
    return clean_sensor, spark_application_id


def process_sensors(payload):
    if 'sensors' in  payload:
        sensors = payload['sensors']
        for sensor in sensors:
            sensor_name = sensor["labels"]["sensor"]
            component = extract_spark_component(sensor_name)
            if component:
                clean_name, app_id = extract_app_id_and_clean_sensor(sensor_name, component)
                labels = sensor["labels"]
                labels['sensor'] = clean_name
                labels['spark_application_id'] = app_id
                labels['spark_component'] = component


def process_line(line):
    try:
        message = json.loads(line)
        payload = json.loads(message['payload'])
        process_sensors(payload)
        message["payload"] = json.dumps(payload)
        json.dump(message, sys.stdout)
        sys.stdout.write('\n')
    except Exception as e:
        error_type = type(e).__name__
        error_msg = f"Error: {error_type}: {str(e)} in line: {line}\n"
        stack_trace = traceback.format_exc()
        error_msg += f"Stack trace:\n{stack_trace}\n"
        sys.stderr.write(error_msg)
        sys.stderr.flush()

        # keep initial message
        sys.stdout.write(line)


def main():
    while True:
        line = sys.stdin.readline().rstrip()
        if len(line) == 0:
            break
        process_line(line)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
