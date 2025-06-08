package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"
	//db "go.etcd.io/bbolt"
)

var symbol = "QQQ"
var domain = "data.alpaca.markets"
var path = "/v2/stocks/" + symbol + "/quotes"
var query = "start=2016-01-01T00%3A00%3A00Z&limit=2&feed=sip&sort=asc"
var fAddress = webQuery(WebQueryAddress{domain: domain, path: path, query: query})
var cfgAlpacaConnect = alpacaCallItOptions{
	url:            fAddress,              //	url string,
	MaxRetries:     3,                     //	maxRetries int,
	maxBackoff:     2 * time.Second,       //	maxBackoff time.Duration,
	initialBackoff: 50 * time.Millisecond, //	initialBackoff time.Duration,
	logText:        "Descarga de Alpaca",  //	logText string
}

func main() {
	res, _ := alpacaCallItWithRetries(cfgAlpacaConnect)
	// decoding country1 struct
	// from json format
	unmarshalGeneric([]byte(res), &thisQuote)

	// una vez sabiendo el nombre del symbolo
	//
	// Initialize DB - default config
	thisDB, err := initDB("write")
	if err != nil {
		log.Fatalf("No se pudo inicializar la base de datos: %v", err)
		fmt.Printf("  :(  ")
	} else {
		fmt.Printf("  =)  ")
	}
	//
	//tx.CreateBucketIfNotExists([]byte("USERS"))

	fmt.Printf("\n\n")
	fmt.Printf("%s\n", thisQuote.Symbol)
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
