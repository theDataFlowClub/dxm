package main

type Quote struct {
	// defining struct variables
	NextPageToken string     `json:"next_page_token"` // Cryptographic value.
	Quotes        []oneQuote `json:"quotes"`          // Quotes Body
	Symbol        string     `json:"symbol"`          // ticker
}

// declaring a struct
type oneQuote struct {
	// defining struct variables
	AP float32 // Ask Price (Precio de Venta).
	AS int32   // Ask Size (Tamaño de Venta).
	AX string  // Ask Exchange (Bolsa de Venta).
	BP float32 // Bid Price (Precio de Compra).
	BS int32   // Bid Size (Tamaño de Compra).
	BX string  // Bid Exchange (Bolsa de Compra).
	C  string  // Conditions (Condiciones de la Operación).
	T  string  // Timestamp (Marca de Tiempo).
	Z  string  // Tape (Cinta).
}

// defining a struct instance
var thisQuote Quote
