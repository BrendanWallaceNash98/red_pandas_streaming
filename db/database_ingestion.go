package main

import (
	"path/filepath"
	"time"
)

var customer struct {
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

var order struct {
	Id            string    `json:"id"`
	CreatedTime   time.Time `json:"created_time"`
	CustomerId    string    `json:"customer_id"`
	OrderProducts string    `json:"order_products"`
	OrderQuantity string    `json:"order_quantity"`
}

func get_file_names(file_pattern string) []string {
	files, err := filepath.Glob(file_pattern)
	if err != nil {
		log.fatal("error finding files with pattern %s \n %v", file_pattern, err)
	}
	return files
}

func main() {
}
