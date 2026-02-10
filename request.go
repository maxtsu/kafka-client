package main

import (
	"fmt"
	"io"
	"net/http"
)

func main() {
	url := "https://example.com"

	// Send GET request
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error making GET request:", err)
		return
	}
	defer resp.Body.Close()

	// Print HTTP status code
	fmt.Println("Status:", resp.Status)

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return
	}

	fmt.Println("Body:")
	fmt.Println(string(body))
}
