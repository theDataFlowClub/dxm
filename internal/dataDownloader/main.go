package main

import (
	"bytes"
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
var fAddress = WebQuery(WebQueryAddress{domain: domain, path: path, query: query})
var LogBuffer bytes.Buffer // Un buffer en memoria para capturar los logs

// defining a struct instance
var thisQuote Quote

func main() {
	var thisDB *db.DB = nil
	// 1. Initialize log process early
	LogInit()
	log.Println("Application started. Logs redirected to in-memory buffer.")

	//
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
	//log.Println("Alpaca data downloaded for symbol: %s", thisQuote.Symbol)

	// 4. Initialize DB with retries
	// Use config.WriteConfig for DB options
	var err error         // Declare err here for main's scope
	var dbInstance *db.DB // Declare a local variable for the DB instance

	// Call initDBWithRetries and assign its result to dbInstance
	dbInstance, err = initDBWithRetries(WriteConfig)
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
	bucket, err := InitBucketWithRetries(
		BkOptions{
			DB_INSTANCE: thisDB,
			BUCKET_NAME: symbol, // Se asume que 'thisQuote' es accesible y tiene un campo 'Symbol'
			ALPACA_QUOTE_BUCKET_SLOTS: oneQuote{
				AP: 0,
				AS: 0,
				AX: "",
				BP: 0,
				BS: 0,
				BX: "",
				C:  "",
				Z:  "",
				T:  "",
			},
		}) // Pass the local config
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize symbol bucket: %v", err)
	}
	log.Printf("Bucket '%s' initialized successfully. Bucket pointer: %v", thisQuote.Symbol, bucket)
	//fmt.Print(thisQuote.Quotes)

	//  'thisQuote' es de tipo QuoteResponse y tiene un campo 'Quotes'
	// que es un slice de alpaca.Quote
	quotesToSave := thisQuote.Quotes
	symbolToSave := thisQuote.Symbol // O QQQ si es constante

	batchSize := len(quotesToSave) // Número de quotes por transacción
	numWorkers := 4                // Número de goroutines concurrentes (ajusta según CPU y IO)

	log.Printf("Iniciando guardado concurrente de %d quotes para %s...", len(quotesToSave), symbolToSave)
	err = SaveQuotesConcurrently(dbInstance, symbolToSave, quotesToSave, batchSize, numWorkers)
	if err != nil {
		log.Printf("Fatal: Error al guardar quotes: %v", err)
	}
	log.Println("Quotes guardadas exitosamente.")

	//
	// ==
	//

	// retrieve the data
	dbInstance.View(func(tx *db.Tx) error {
		// 1. Obtén el bucket principal del símbolo (ej. "QQQ")
		symbolBucket := tx.Bucket([]byte(symbol))
		if symbolBucket == nil {
			return fmt.Errorf("Bucket '%s' no encontrado", symbol)
		}

		// Nombres de los sub-buckets para iterar
		fieldBuckets := []string{"AP", "AS", "AX", "BP", "BS", "BX", "C", "Z"}

		// 2. Itera sobre cada sub-bucket dentro del bucket del símbolo
		for _, fieldName := range fieldBuckets {
			subBucket := symbolBucket.Bucket([]byte(fieldName))
			if subBucket == nil {
				// Esto podría pasar si un sub-bucket aún no se ha creado
				// (ej. si no hay datos para ese campo todavía)
				fmt.Printf("Sub-bucket '%s' para símbolo '%s' no encontrado.\n", fieldName, symbol)
				continue // Pasa al siguiente sub-bucket
			}

			fmt.Printf("--- Datos para Símbolo: %s, Campo: %s ---\n", symbol, fieldName)

			// 3. Dentro de cada sub-bucket, itera sobre los pares clave-valor (timestamp -> valor del campo)
			err := subBucket.ForEach(func(k, v []byte) error {
				// Convierte la clave (timestamp Unix Nano []byte) de nuevo a int64 y luego a time.Time para legibilidad
				unixNano := int64(binary.BigEndian.Uint64(k))
				t := time.Unix(0, unixNano) // Convertir a time.Time

				// Imprime la clave (timestamp) y el valor
				// Nota: v es []byte. Si sabes el tipo original, puedes convertirlo de nuevo.
				// Por ejemplo, si es un float, strconv.ParseFloat. Si es JSON, json.Unmarshal.
				fmt.Printf("  Key (Timestamp): %s, Value: %s\n", t.Format(time.RFC3339Nano), string(v))
				return nil
			})
			if err != nil {
				return fmt.Errorf("Error al iterar sub-bucket '%s': %w", fieldName, err)
			}
		}

		return nil
	})
	/*
		for i, q := range thisQuote.Quotes {
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
			binary.BigEndian.PutUint64(key, uint64(unixNano)) // Cast to uint64 for PutUint6
			fmt.Printf("--- Quote %d ---\n", i+1)
			fmt.Printf("  AP: %f, AS: %d, AX: %s\n", q.AP, q.AS, q.AX)
			fmt.Printf("  BP: %f, BS: %d, BX: %s\n", q.BP, q.BS, q.BX)
			fmt.Printf("  Conditions: %v\n", q.C)
			fmt.Printf("  Tape: %v\n", q.Z)
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
	*/
	defer thisDB.Close() // ¡Recuerda cerrar la DB!
}
