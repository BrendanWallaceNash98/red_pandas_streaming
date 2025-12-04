package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Customer struct {
	Id           string `json:"id"`
	CreatedTime  string `json:"created_time"`
	FullName     string `json:"full_name"`
	Salulation   string `json:"salulation"`
	FirstName    string `json:"first_name"`
	LastName     string `json:"last_name"`
	FullAddress  string `json:"full_address"`
	StreetNumber int    `json:"street_number"`
	StreetName   string `json:"street_name"`
	City         string `json:"city"`
	Postcode     string `json:"postcode"`
	State        string `json:"state"`
}

type OrderRaw struct {
	Id               string          `json:"id"`
	CreatedTime      string          `json:"created_at"`
	CustomerId       string          `json:"customer_id"`
	RawOrderProducts json.RawMessage `json:"order_products"`
	RawOrderQuantity json.RawMessage `json:"order_quantity"`
}

type Order struct {
	Id            string `json:"id"`
	CreatedTime   string `json:"created_at"`
	CustomerId    string `json:"customer_id"`
	OrderProducts string
	OrderQuantity string
}

func (o *OrderRaw) ParseOrder() Order {
	var order Order
	order.Id = o.Id
	order.CreatedTime = o.CreatedTime
	order.CustomerId = o.Id
	order.OrderQuantity = string(o.RawOrderQuantity)
	order.OrderQuantity = string(o.RawOrderProducts)
	return order
}

func getFileNames(file_pattern string) []string {
	files, err := filepath.Glob(file_pattern)
	if err != nil {
		log.Fatal("error finding files with pattern")
	}
	return files
}

func createFileChannel(filePath string) chan string {
	fileCh := make(chan string, 10000)
	go func(string, chan string) {
		for _, file := range getFileNames(filePath) {
			fileCh <- file
		}
	}(filePath, fileCh)
	return fileCh
}

func parseDataFromFiles[T Order | Customer](fileChan chan string, dataChan chan T) chan error {
	errChan := make(chan error, 100)
	go func(fileChan chan string, dataChan chan T, errChan chan error) {
		for fileName := range fileChan {
			jsonData, err := os.ReadFile(fileName)
			if err != nil {
				errChan <- err
				continue
			}

			var genType T
			getTypePtr := any(&genType)
			// Allocate the correct target based on T
			switch v := getTypePtr.(type) {
			case *Order:
				var marshalData OrderRaw
				err = json.Unmarshal(jsonData, &marshalData)
				if err != nil {
					errChan <- err
				}
				*v = marshalData.ParseOrder()

			case *Customer:
				var marshalData Customer
				err = json.Unmarshal(jsonData, &marshalData)
				if err != nil {
					errChan <- err
				}
				*v = marshalData
			default:
				errChan <- fmt.Errorf("unsupported type")
			}

			dataChan <- genType
		}
	}(fileChan, dataChan, errChan)

	return errChan
}

func databaseConn() (*sql.DB, error) {
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(context.Background()); err != nil {
		return nil, err
	}

	return db, err
}

type sqlParams interface {
	sqlValues() []interface{}
}

func (cstmr Customer) sqlValues() []interface{} {
	return []interface{}{
		cstmr.Id,
		cstmr.CreatedTime,
		cstmr.FullName,
		cstmr.Salulation,
		cstmr.FirstName,
		cstmr.LastName,
		cstmr.FullAddress,
		cstmr.StreetNumber,
		cstmr.StreetName,
		cstmr.City,
		cstmr.Postcode,
		cstmr.State,
	}
}

func (ordr Order) sqlValues() []interface{} {
	return []interface{}{
		ordr.Id,
		ordr.CreatedTime,
		ordr.CustomerId,
		ordr.OrderProducts,
		ordr.OrderQuantity,
	}
}

func insertData[T sqlParams](db *sql.DB, queryTemp string, data T) error {
	_, err := db.Exec(queryTemp, data.sqlValues()...)
	return err
}

func processInserts[T sqlParams](queryTemp string, db *sql.DB, dataChan chan T) chan error {
	errChan := make(chan error, 1000)
	defer close(errChan)
	workers := 4
	wg := new(sync.WaitGroup)
	for range workers {
		wg.Add(1)
		go func(db *sql.DB, queryTemp string, dataChan chan T) {
			for data := range dataChan {
				err := insertData(db, queryTemp, data)
				if err != nil {
					errChan <- err
				}
			}
		}(db, queryTemp, dataChan)

		go func(wg *sync.WaitGroup, errChan chan error) {
			wg.Wait()
			close(errChan)
		}(wg, errChan)
	}
	return errChan
}

func main() {
	dir := `data_creation/data/`
	// file_types := []string{`order*.json`, `customer*.json`}
	file_type := `order*.json`
	pattern := dir + file_type
	filepath.Join(dir, pattern)
	files := getFileNames(pattern)
	for _, file_path := range files {
		jsonData, err := os.ReadFile(file_path)
		if err != nil {
			fmt.Printf("error reading the file: %v", err)
			continue
		}
		var json_order OrderRaw
		err = json.Unmarshal(jsonData, &json_order)
		if err != nil {
			fmt.Printf("error marshhalling the data: %v", err)
			continue
		}
		clean_order := json_order.ParseOrder()
		fmt.Println(clean_order)
	}
}
