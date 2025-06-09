package main

// Import necessary packages for data processing and database operations
import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	db "go.etcd.io/bbolt"
)

var symbol = "QQQ"
var domain = "data.alpaca.markets"
var path = "/v2/stocks/" + symbol + "/quotes"
var query = "start=2016-01-01T00%3A00%3A00Z&limit=2&feed=sip&sort=asc"
var fAddress = webQuery(WebQueryAddress{domain: domain, path: path, query: query})

func main() {
	// 1. Initialize log process early
	logInit()
	log.Println("Application started. Logs redirected to in-memory buffer.")

	// 2. Call Alpaca API with retries
	// Using config.cfgAlpacaConnect assuming it's in your config package
	res, _ := alpacaCallItWithRetries(
		alpacaCallItOptions{
			url:            fAddress,              //	url string,
			MaxRetries:     3,                     //	maxRetries int,
			maxBackoff:     2 * time.Second,       //	maxBackoff time.Duration,
			initialBackoff: 50 * time.Millisecond, //	initialBackoff time.Duration,
			logText:        "Descarga de Alpaca",  //	logText string
		})

	// 3. Decode the Alpaca response into thisQuote
	// This populates thisQuote.Symbol, which is needed later
	unmarshalGeneric([]byte(res), &thisQuote)
	log.Println("Alpaca data downloaded for symbol: %s", thisQuote.Symbol)

	// 4. Initialize DB with retries
	// Use config.writeConfig for DB options
	var err error         // Declare err here for main's scope
	var dbInstance *db.DB // Declare a local variable for the DB instance

	// Call initDBWithRetries and assign its result to dbInstance
	dbInstance, err = initDBWithRetries(writeConfig)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize database: %v", err)
	}
	// Set the global thisDB AFTER it's successfully initialized
	thisDB = dbInstance // <--- THIS IS THE CRITICAL LINE!
	log.Println("Database initialized successfully.")

	// IMPORTANT: Defer closing the DB connection
	defer func() {
		if thisDB != nil {
			closeErr := thisDB.Close()
			if closeErr != nil {
				log.Printf("Error closing database: %v", closeErr)
			} else {
				log.Println("Database closed successfully.")
			}
		}
	}()

	// 5. Load the symbol bucket
	// NOW, initialize thisTickerUpdater with the VALID dbInstance
	// You cannot use the global `config.thisTickerUpdater` directly if it was initialized
	// with a nil `dbInstance` at package-level. You need to create a new one, or
	// modify the existing one.
	bucket, err := initBucketWithRetries(
		bkOptions{
			dbInstance: thisDB,
			bucketName: thisQuote.Symbol, // Se asume que 'thisQuote' es accesible y tiene un campo 'Symbol'
			alpacaQuoteBuketSlots: alpacaQuoteBuketSlots{
				ap: "",
				as: "",
				ax: "",
				bp: "",
				bs: "",
				bx: "",
				c:  "",
				z:  "",
				t:  "",
			},
		}) // Pass the local config
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize symbol bucket: %v", err)
	}
	log.Printf("Bucket '%s' initialized successfully. Bucket pointer: %v", thisQuote.Symbol, bucket)
	//
	for i, q := range thisQuote.Quotes {
		fmt.Printf("--- Quote %d ---\n", i+1)
		fmt.Printf("  AP: %f, AS: %d, AX: %s\n", q.AP, q.AS, q.AX)
		fmt.Printf("  BP: %f, BS: %d, BX: %s\n", q.BP, q.BS, q.BX)
		fmt.Printf("  Conditions: %v\n", q.C)
		fmt.Printf("  Tape: %v\n", q.Z)
		//
		// timestamp parsing
		// Example timestamp from Alpaca
		timestampStr := q.T

		// Go's time package understands RFC3339 very well
		t, err := time.Parse(time.RFC3339Nano, timestampStr) // Use RFC3339Nano for sub-second precision
		if err != nil {
			log.Fatalf("Error parsing timestamp: %v", err)
		}
		// Convert time.Time to int64 Unix Nanoseconds
		unixNano := t.UnixNano() // This gives you an int64
		// This int64 represents the number of nanoseconds since January 1, 1970 UTC.
		// This number *always* increases monotonically with time.
		key := make([]byte, 8)                            // An int64 is 8 bytes
		binary.BigEndian.PutUint64(key, uint64(unixNano)) // Cast to uint64 for PutUint64
		//
		fmt.Printf("  Timestamp: %v\n", unixNano)
		//
		//
		//
		// INICIA INYECCION DE DATOS
		//
		//
		//
		//
		// SIEMPRE SE CIERRA TRAS LA OPERACION
		defer thisDB.Close() // ¡Recuerda cerrar la DB!

	}

}
