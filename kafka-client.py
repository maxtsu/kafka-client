#!/usr/bin/python3
import signal
import yaml

from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer


## Global
kill_signal = False
number_consumers = 4
yaml_file = "kafka-config.yaml"

with open(yaml_file, 'r') as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)
if "number.consumers" in config:
    number_consumers = config["number.consumers"]

brokers = config["bootstrap.servers"].split(',')

#SIGINT to terminate the program
def handler_stop_signals(signum, frame):
    print("Received SIGINT signal")
    global kill_signal
    kill_signal = True
signal.signal(signal.SIGINT, handler_stop_signals)

#consumer function
def consumer(id):
    print("Consumer {} Starting kafka".format(id))
    global kill_signal
    run = True
    while run:
        try:
            #Connect to Kafka broker and topic
            consumer = KafkaConsumer(config["topics"], bootstrap_servers=brokers, \
                        sasl_plain_username=config["sasl.username"], sasl_plain_password=config["sasl.password"], \
                        security_protocol=config["security.protocol"], sasl_mechanism=config["sasl.mechanisms"], \
                        ssl_cafile=config["ssl.ca.location"], auto_offset_reset = config["auto.offset.reset"])
            print("Connected to brokers")
            for msg in consumer:
                telemetry_msg = msg.value
                print("{}".format(str(telemetry_msg)))
                if kill_signal:
                    run = False
                    print("consumer {} received termination".format(id))
                    break
        except Exception as e:
            print("Consumer {} Error when connecting with kafka consumer: {}".format(id, str(e)))
        finally:
            print("Unsubscribe and close kafka consumer {}".format(id))
            consumer.unsubscribe()
            consumer.close()
    run = False

with ThreadPoolExecutor(max_workers=number_consumers) as executor:
        executor.map(consumer, range(number_consumers))

print("Main thread completed")