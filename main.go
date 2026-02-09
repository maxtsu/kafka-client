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
	// Choose only the fields you care about
	want := []string{"time", "cfs-id", "key1", "key2", "index"}

	// Execute
	res, err := queryDB(InfluxClient, client.NewQuery(q, db, "ns")) // precision: "ns","u","ms","s","m","h"
	if err != nil {
		log.Errorf("Influx query error %+v", err)
	} else { // no query error
		fmt.Printf("\n%+v\n", res)

		rows, err := ExtractSelectedFromResults(res, want)
		if err != nil {
			log.Errorf("unable to extract results %+v", err)
		}
		fmt.Printf("\n%+v\n", rows)

		// Print a few rows
		for _, r := range rows {
			fmt.Printf("time=%v key1=%v key2=%v index=%v deviceTS=%v offset=%v\n",
				r["time"], r["key1"], r["key2"], r["index"], r["__device_timestamp__"], r["tandTimeOffset"])
		}

	}
}

// colIndexMap builds a name->index map for quick and safe lookups.
func colIndexMap(cols []string) map[string]int {
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[c] = i
	}
	return m
}

// ExtractSelectedFromResults extracts only the requested fields from a slice of client.Result.
// It returns a slice of maps, each map = one row, with only the "want" fields present.
func ExtractSelectedFromResults(results []client.Result, want []string) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, 0, 128)

	for ri, r := range results {
		if r.Err != "" {
			return nil, fmt.Errorf("result[%d] error: %s", ri, r.Err)
		}
		for _, s := range r.Series {
			idx := colIndexMap(s.Columns)
			for _, row := range s.Values {
				rec := make(map[string]interface{}, len(want))
				for _, w := range want {
					if j, ok := idx[w]; ok && j < len(row) {
						rec[w] = row[j]
					}
				}
				out = append(out, rec)
			}
		}
	}
	return out, nil
}

// // /////////////////////////////
// // wrap []client.results into client.response
// func WrapResults(results []client.Result) *client.Response {
// 	return &client.Response{
// 		Results: results,
// 		Err:     "",
// 	}
// }

// func extractFields(res *client.Response) error {
// 	if res == nil {
// 		return fmt.Errorf("nil response")
// 	}
// 	if res.Err != "" {
// 		return fmt.Errorf("response error: %s", res.Err)
// 	}
// 	if len(res.Results) == 0 || len(res.Results[0].Series) == 0 {
// 		fmt.Println("no series returned")
// 		return nil
// 	}

// 	s := res.Results[0].Series[0] // assuming a single series; loop if multiple
// 	cols := s.Columns

// 	// Pick the fields you need
// 	idxTime := mustColIdx(cols, "time")
// 	idxKey1 := mustColIdx(cols, "key1")
// 	idxKey2 := mustColIdx(cols, "key2")
// 	idxIndex := mustColIdx(cols, "index")
// 	idxDeviceTS := mustColIdx(cols, "__device_timestamp__")
// 	idxOffset := mustColIdx(cols, "tandTimeOffset")

// 	for _, row := range s.Values {
// 		// time is usually returned as RFC3339 string; numeric (ns) only if you used epoch + precision
// 		tsStr, _ := row[idxTime].(string)

// 		key1 := toString(row[idxKey1])
// 		key2 := toString(row[idxKey2])
// 		idxVal := toString(row[idxIndex])
// 		devTS := toInt64(row[idxDeviceTS]) // your data shows a 19-digit ns value
// 		offset := toString(row[idxOffset]) // e.g., "13.305877ms"

// 		fmt.Printf("time=%s key1=%s key2=%s index=%s deviceTS=%d offset=%s\n",
// 			tsStr, key1, key2, idxVal, devTS, offset)
// 	}
// 	return nil
// }

/////////////////////////////////////////////////

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
