# kafka-client sarama v2.1
kafka golang client  
Docker file is required for building a container image of the application
To create an alpine version
    - docker build -t my-kafka-container .
    - docker create --name extract my-kafka-container
    - docker cp extract:/kafka.app ./kafka-alpine.app
Configuration parameters in the file kafka-config.yaml  
Can select producer or consumer Defined in the yaml file  
In producer more messages are pasted to console  

- 2.0 16/03/2026
    - Create suicide timeout app will kill itself after 10min
- 2.1 19/05/2026
    - Allow config file to be *.yaml or *.yml 