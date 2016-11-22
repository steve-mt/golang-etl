package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const orderSource = "./orders.csv"
const productSource = "./productList.csv"
const destPath = "./dest.csv"

func main() {
	// Just to see how long it took
	start := time.Now()

	extractCh := make(chan *Order)
	transformCh := make(chan *Order)
	doneCh := make(chan bool)

	go extract(extractCh)
	go transform(extractCh, transformCh)
	go load(transformCh, doneCh)

	<-doneCh
	// Show the user how long the etl process took
	fmt.Println(time.Since(start))
}

// Product information.
type Product struct {
	PartNumber string
	UnitCost   float64
	UnitPrice  float64
}

// Order information.
// Has a relationship with Product.
type Order struct {
	CustomerNumber int
	PartNumber     string
	Quantity       int

	UnitCost  float64
	UnitPrice float64
}

// Extracts the data from orders.
func extract(extractCh chan *Order) {
	// Get all the inform from orders.
	f, err := os.Open(orderSource)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	// Load the csv format.
	r := csv.NewReader(f)

	// Go trough every record we have.
	for record, err := r.Read(); err == nil; record, err = r.Read() {
		// Create a new order with the data.
		order := new(Order)
		order.CustomerNumber, _ = strconv.Atoi(record[0])
		order.PartNumber = record[1]
		order.Quantity, _ = strconv.Atoi(record[2])
		// Send it to the channel.
		extractCh <- order
	}

	// Close the channel.
	close(extractCh)
}

// Transform the orders that are in the extract channel.
func transform(extractCh, transformCh chan *Order) {
	f, err := os.Open(productSource)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Create a new map with products.
	productList := make(map[string]*Product)
	for _, record := range records {
		product := new(Product)
		product.PartNumber = record[0]
		product.UnitCost, _ = strconv.ParseFloat(record[1], 64)
		product.UnitPrice, _ = strconv.ParseFloat(record[2], 64)
		productList[product.PartNumber] = product
	}

	// Set up a wait group.
	var waitGrp sync.WaitGroup
	// Go trough every record in the extract channel.
	for o := range extractCh {
		// Add a wait group.
		waitGrp.Add(1)
		// For each record in the extract channel.
		go func(o *Order) {
			o.UnitCost = productList[o.PartNumber].UnitCost
			o.UnitPrice = productList[o.PartNumber].UnitPrice
			transformCh <- o
			waitGrp.Done()
		}(o)
	}

	waitGrp.Wait()

	close(transformCh)
}

// Load the data into a text file.
func load(transformCh chan *Order, doneCh chan bool) {
	f, err := os.Create(destPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Add a header to the text file.
	_, err = fmt.Fprintf(f, "%20s%15s%12s%12s%15s%15s\n",
		"Part Number", "Quantity",
		"Unit Cost", "Unit Price",
		"Total Cost", "Total Price")

	if err != nil {
		log.Fatal(err)
	}

	// Set up a wait group.
	var waitGrp sync.WaitGroup
	// Go trough every record in the channel.
	for o := range transformCh {
		waitGrp.Add(1)
		go func(o *Order) {
			_, err = fmt.Fprintf(f, "%20s %15d %12.2f %12.2f %15.2f %15.2f\n",
				o.PartNumber, o.Quantity,
				o.UnitCost, o.UnitPrice,
				o.UnitCost*float64(o.Quantity),
				o.UnitPrice*float64(o.Quantity))
			if err != nil {
				log.Fatal(err)
			}
			waitGrp.Done()
		}(o)

	}

	waitGrp.Wait()

	// Tell the main function that we are done.
	doneCh <- true
}
