package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// Version 0.9
const config_file = "kafka-config.yaml"

func main() {
	fmt.Println("influx application v0.1")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// // Rad the config file
	// byteResult := ReadFile(config_file)

	// err := yaml.Unmarshal(byteResult, &configYaml)
	// if err != nil {
	// 	fmt.Println("kafka-config.yaml Unmarshall error", err)
	// }
	// fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

}

// Function to read text file return byteResult
func ReadFile(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	byteResult, _ := io.ReadAll(file)
	file.Close()
	return byteResult
}
