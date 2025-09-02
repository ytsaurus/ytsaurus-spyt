#! /usr/bin/env python3
import json
import sys
import traceback


def extract_spark_component(sensor):
    spark_component = None
    if '_driver_' in sensor:
        spark_component = 'driver'
    elif '_executor_' in sensor:
        spark_component = 'executor'
    return spark_component


def extract_app_id_and_clean_sensor(sensor, spark_component, spark_component_label_missed=False):
    sensor_parts = sensor.split('_')
    spark_application_id = sensor_parts[1]
    length_to_delete = 3 if spark_component == 'driver' else 4
    if spark_component_label_missed:
        length_to_delete -= 1
    del sensor_parts[1:length_to_delete]
    clean_sensor = "_".join(sensor_parts)
    return clean_sensor, spark_application_id


def process_sensors(payload):
    if 'sensors' in  payload:
        sensors = payload['sensors']
        component = payload["commonLabels"]['component']
        for sensor in sensors:
            sensor_name = sensor["labels"]["sensor"]
            is_spark_component_label_missed = False
            spark_component = extract_spark_component(sensor_name)
            if spark_component is None and component in ('driver', 'executor'):
                # special case with missed executor tag in name we inherit from job task name
                is_spark_component_label_missed = True
                spark_component = component
            if spark_component:
                clean_name, app_id = extract_app_id_and_clean_sensor(sensor_name, spark_component,
                                                                     is_spark_component_label_missed)
                labels = sensor["labels"]
                labels['sensor'] = clean_name
                labels['spark_application_id'] = app_id
                labels['spark_component'] = spark_component


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
