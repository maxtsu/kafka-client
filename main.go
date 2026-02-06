package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/gologme/log"
	client "github.com/influxdata/influxdb1-client/v2"
)

// Version 0.9
const config_file = "kafka-config.yaml"

var InfluxClient client.Client // Influx client
var tand_host = "172.16.16.21"
var tand_port = "28086"

func main() {
	fmt.Println("influx application v0.1")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Set logging level
	level := 5 //debug level
	log.EnableLevelsByNumber(level)
	log.EnableFormattedPrefix()
	log.Infoln("Logging configured Set at level ", level)

	// // Rad the config file
	// byteResult := ReadFile(config_file)

	// err := yaml.Unmarshal(byteResult, &configYaml)
	// if err != nil {
	// 	fmt.Println("kafka-config.yaml Unmarshall error", err)
	// }
	// fmt.Printf("kafka-config.yaml: %+v\n", configYaml)

	//Create InfluxDB client
	InfluxCreateClient(tand_host, tand_port)
	log.Infof("Influx client create %+v\n", InfluxClient)

	// Build query
	db := "hb-default:my-group01:192.168.100.100"
	measurement := "external/upload_test01"
	q := fmt.Sprintf(`
        SELECT * FROM %q
    `, measurement)

	// Execute
	res, err := queryDB(InfluxClient, client.NewQuery(q, db, "ns")) // precision: "ns","u","ms","s","m","h"
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n%+v\n", res)

}

func queryDB(cl client.Client, q client.Query) ([]client.Result, error) {
	resp, err := cl.Query(q)
	if err != nil {
		return nil, err
	}
	if resp.Error() != nil {
		return nil, resp.Error()
	}
	return resp.Results, nil
}

// Create InfluxDB client Global variable InfluxClient
func InfluxCreateClient(tand_host string, tand_port string) {
	// Make Influx client
	url := "http://" + tand_host + ":" + tand_port
	var err error
	InfluxClient, err = client.NewHTTPClient(client.HTTPConfig{
		Addr: url,
	})
	if err != nil {
		log.Errorln("Error creating InfluxDB Client: ", err.Error())
	}
	defer InfluxClient.Close()
	log.Infoln("InfluxDB Client connection", InfluxClient)
	//return influxClient
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
