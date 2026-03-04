# kafka-client sarama v0.1
kafka golang client  
Docker file is required for building a container image of the application
To create an alpine version
    - docker build -t my-kafka-container .
    - docker create --name extract my-kafka-container
    - docker cp extract:/kafka.app ./kafka-alpine.app
Configuration parameters in the file kafka-config.yaml  
Can select producer or consumer Defined in the yaml file  
In producer more messages are pasted to console  
