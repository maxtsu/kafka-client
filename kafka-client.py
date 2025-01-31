#!/usr/bin/env python
import json
import signal
import time

from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer


#consumer function
def consumer():
    run = True
    while run:
        try:
            #Connect to Kafka broker and topic
            consumer = KafkaConsumer(topics, bootstrap_servers=[brokers], sasl_plain_username=user, sasl_plain_password=password, security_protocol=secprotocol, sasl_mechanism=saslmech)
            # iterate through kafka messages
            for msg in consumer:
                print('Received kafka message {}'.format(msg))
                # Load Kafa message to Json
                telemetry_msg = msg.value
                print('Processing kafka message {}'.format(str(telemetry_msg)[:300]))
                ### Telemetry Data message in JSON format
                telemetry_msg_json = json.loads(telemetry_msg)
                ### Extract source, path, port, key_value and timestamp from message
                ### ... key_values are a dictionary of {folder,{key1:value1,key2:value2},etc.} extracted from path of message
                ### e.g. 'interfaces/interface[name=ge-0/0/1]/state' interface name ge-0/0/1 is in folder 'interface'
                content_source, sensor_path_message, port, key_values, timestamp = getSource(telemetry_msg_json)
                ### Create name-path string for matching configuration tuple
                messageNamePath = content_source + '-' + sensor_path_message

                ### Search configuration for this name-path pair
                ### Check tuple inside tuple
                ### If there is no match to configuration tuples. Message is ignored
                matching_rule = 'Null'
                for item in configuration:
                    if messageNamePath in item:
                        matching_rule = item # Matching item from config.json
                        matchInRulesJSON = False # Flag for a match in the rules.json file
                        #Find details for the rule in the rules.JSON
                        for rule in rulesJSON: #iterate through all the rules in rulesJSON
                            # Match on the rule-id between config.json & rules.json
                            if matching_rule[6] == rule['rule-id']:
                                matchInRulesJSON = True # Set flag to true
                                logging.info('Process rule {}'.format(rule['rule-id']))
                                fields =dataPointsFromMessage(telemetry_msg_json, rule)
                                # Extract index_values list from rules.json tuple
                                for index in rule['index_values']: #iterate across all index values
                                    # extract the index values from the message path string
                                    value = kvs_dict_parse(key_values, index['path'] , index['index'])
                                    fields.update({index['index']:value})
                                if not 'Null' in fields: # As long as there is no 'Null' in fields write to TSDB
                                    # Take data and write to TSDB create thread for TSDB write. Call function with DB fields
                                    # Thread limit with semaphore
                                    thread = threading.Thread(target=writeTSDB, args=(tand_host, tand_port, matching_rule, timestamp, fields, semaphore))
                                    thread.start()
                            else:
                                continue # continue with next iteration for a match
                        if matchInRulesJSON == False: #no path match for telemetry message
                            #does not match any rule
                            logging.info('Message no matching rule in rules.json {}'.format(str(telemetry_msg_json)[:300]))
                if matching_rule == 'Null':
                    # log ignored message
                    logging.info('Ignore kafka message {}'.format(str(telemetry_msg)[:300]))
                if not run:  # Break with SIGHUP
                    logging.info('Received SIGHUP breaking from kafka messaging loop')
                    break
        except Exception as e:
            logging.critical('Error when connecting with kafka consumer: ' + str(e))
