package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type Customer struct {
	Id           string    `json:"id"`
	CreatedTime  time.Time `json:"created_time"`
	FullName     string    `json:"full_name"`
	Salulation   string    `json:"salulation"`
	FirstName    string    `json:"first_name"`
	LastName     string    `json:"last_name"`
	FullAddress  string    `json:"full_address"`
	StreetNumber int       `json:"street_number"`
	StreetName   string    `json:"street_name"`
	City         string    `json:"city"`
	Postcode     string    `json:"postcode"`
	State        string    `json:"state"`
}

type order struct {
	Id            string    `json:"id"`
	CreatedTime   time.Time `json:"created_time"`
	CustomerId    string    `json:"customer_id"`
	OrderProducts string    `json:"order_products"`
	OrderQuantity string    `json:"order_quantity"`
}

func get_file_names(file_pattern string) []string {
	files, err := filepath.Glob(file_pattern)
	if err != nil {
		log.Fatal("error finding files with pattern %s \n %v", file_pattern, err)
	}
	return files
}

func main() {
	dir := `data_creation/data/`
	file_type := `customer*.json`
	pattern := dir + file_type
	filepath.Join(dir, pattern)
	files := get_file_names(pattern)
	for _, file_path := range files {
		jsonData, err := os.ReadFile(file_path)
		if err != nil {
			fmt.Errorf("error reading the file: %v", err)
		}
		var cstmr Customer
		err = json.Unmarshal(jsonData, &cstmr)
		if err != nil {
			fmt.Errorf("error marshhalling the data: %v", err)
		}
		fmt.Println(cstmr)
	}
}
