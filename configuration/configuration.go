package configuration

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
)

// configuration file kafka-config.yaml
type Config struct {
	Timestamp         bool   `yaml:"timestamp"`
	Producer          bool   `yaml:"producer"`
	BootstrapServers  string `yaml:"bootstrap.servers"`
	SaslMechanisms    string `yaml:"sasl.mechanisms"`
	SecurityProtocol  string `yaml:"security.protocol"`
	SaslUsername      string `yaml:"sasl.username"`
	SaslPassword      string `yaml:"sasl.password"`
	SslCaLocation     string `yaml:"ssl.ca.location"`
	GroupID           string `yaml:"group.id"`
	Topics            string `yaml:"topics"`
	AutoOffset        string `yaml:"auto.offset.reset"`
	PartitionStrategy string `yaml:"partition.assignment.strategy"`
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

func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// // Load client cert
	// cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	// if err != nil {
	// 	return &tlsConfig, err
	// }
	// tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	//caCert, err := ioutil.ReadFile(caCertFile)
	caCert := ReadFile(caCertFile)
	// if err != nil {
	// 	return &tlsConfig, err
	// } else {
	// 	fmt.Printf("CA cert file load error %+v", err)
	// }
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	//tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}
