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
	"github.com/joho/godotenv"
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

func getFileNames(filePattern string) []string {
	files, err := filepath.Glob(filePattern)
	if err != nil {
		log.Fatal("error finding files with pattern")
	}
	return files
}

func createFileChannel(filePattern string) chan string {
	fileCh := make(chan string, 10000)
	go func(filerPattern string, fileChan chan string) {
		defer close(fileChan)
		for _, file := range getFileNames(filePattern) {
			fileCh <- file
		}
	}(filePattern, fileCh)
	return fileCh
}

func parseDataFromFiles[T Order | Customer](fileChan <-chan string, dataChan chan<- T) chan error {
	errChan := make(chan error, 100)
	go func(fileChan <-chan string, dataChan chan<- T, errChan chan error) {
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
	databaseUrl := os.Getenv("DATABASE_URL")
	fmt.Printf("database url is : %v", databaseUrl)
	db, err := sql.Open("pgx", databaseUrl)
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
	result, err := db.Exec(queryTemp, data.sqlValues()...)
	fmt.Printf("sql result is : n/ %v", result)
	if err != nil {
		log.Printf("sql error: %v", err)
	} else {
		log.Printf("succes???: %v", result)
	}
	return err
}

func processInserts[T sqlParams](db *sql.DB, queryTemp string, dataChan chan T) chan error {
	errChan := make(chan error, 1000)
	workers := 4
	wg := new(sync.WaitGroup)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(db *sql.DB, queryTemp string, dataChan chan T) {
			defer wg.Done()
			for data := range dataChan {
				err := insertData(db, queryTemp, data)
				if err != nil {
					errChan <- err
				}
			}
		}(db, queryTemp, dataChan)
	}
	go func(wg *sync.WaitGroup, errChan chan error) {
		wg.Wait()
		close(errChan)
	}(wg, errChan)

	return errChan
}

func main() {
	// dir := `data_creation/data/`
	// file_types := []string{`order*.json`, `customer*.json`}
	err := godotenv.Load() // loads .env from current working directory
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	var file_wg sync.WaitGroup

	fmt.Println("starting process")

	file_wg.Add(1)
	go func(fp string) {
		defer file_wg.Done()

		fileChan := createFileChannel(fp)
		dataChan := make(chan Order, 1000)
		fmt.Println("starting file parse")
		fileParseErrChan := parseDataFromFiles(fileChan, dataChan)
		fmt.Println("creating database connection")
		dbConn, err := databaseConn()
		if err != nil {
			log.Fatal(`failed to connect to database`)
		}
		fmt.Println("processing insers")
		insertQuery := `INSERT INTO staging.orders (id, created_date, customer_id, order_products, order_quantity) VALUES ($1, $2, $3, $4, $5);`
		dbInsertErrChan := processInserts(dbConn, insertQuery, dataChan)

		fmt.Println("moving on to errChans")
		var errWg sync.WaitGroup
		errWg.Add(1)
		go func(errChan <-chan error, wg *sync.WaitGroup) {
			defer errWg.Done()
			defer close(fileParseErrChan)
			for i := range errChan {
				log.Fatal(i)
			}
		}(fileParseErrChan, &errWg)

		errWg.Add(1)
		go func(errChan <-chan error, wg *sync.WaitGroup) {
			defer errWg.Done()
			defer close(dbInsertErrChan)
			for i := range errChan {
				log.Fatal(i)
			}
		}(dbInsertErrChan, &errWg)

		errWg.Wait()
	}(`data_creation/data/order*.json`)

	file_wg.Wait()

	/*

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
	*/
}
