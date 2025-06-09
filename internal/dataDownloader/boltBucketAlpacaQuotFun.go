package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	db "go.etcd.io/bbolt"
)

// declaring a struct
type oneQuote struct {
	// defining struct variables
	AP float64 `json:"ap"` // Ask Price (Precio de Venta).
	AS int     `json:"as"` // Ask Size (Tamaño de Venta).
	AX string  `json:"ax"` // Ask Exchange (Bolsa de Venta).
	BP float64 `json:"bp"` // Bid Price (Precio de Compra).
	BS int     `json:"bs"` // Bid Size (Tamaño de Compra).
	BX string  `json:"bx"` // Bid Exchange (Bolsa de Compra).
	C  string  `json:"c"`  // Conditions (Condiciones de la Operación).
	T  string  `json:"t"`  // Timestamp (Marca de Tiempo).
	Z  string  `json:"z"`  // Tape (Cinta).
}

/*
//
//
//

BLOQUE DE initBucket
// ===============================

//
//
//
*/

// bkOptions es una estructura que agrupa los parámetros necesarios para
// identificar y operar sobre un bucket específico en una base de datos bbolt.
// Se utiliza típicamente para pasar estos parámetros a funciones que interactúan
// con buckets, como `initBucketWithRetries`.
type BkOptions struct {
	// dbInstance es un puntero a la instancia de la base de datos bbolt
	// a la que pertenece el bucket.
	DB_INSTANCE *db.DB
	// bucketName es el nombre del bucket dentro de la base de datos,
	// representado como uSna cadena.
	BUCKET_NAME string
	//
	ALPACA_QUOTE_BUCKET_SLOTS oneQuote
}

// initBucket obtiene o crea un bucket dentro de una base de datos bbolt.
//
// Un bucket en bbolt es una colección de pares clave-valor. Si el bucket
// especificado por 'bucketName' ya existe en la base de datos, esta función
// lo recupera. Si no existe, lo crea.
//
// Esta operación se realiza dentro de una transacción de escritura ('db.Update')
// para asegurar la consistencia y la seguridad de los datos.
//
// Parámetros:
//   - dbInstance: Un puntero a la instancia de la base de datos bbolt ('*bolt.DB')
//     donde se buscará o creará el bucket.
//   - bucketName: El nombre del bucket que se desea obtener o crear, como una cadena.
//     Los nombres de los buckets se almacenan como []byte en bbolt, por lo que se convierte
//     internamente.
//
// Devuelve:
//   - Un puntero a '*bolt.Bucket' si la operación es exitosa, lo que permite
//     interactuar con el bucket (ej., poner o obtener valores).
//   - 'nil' y un 'error' si ocurre algún problema durante la transacción
//     (ej., la base de datos no está abierta o hay un conflicto).
//
// Ejemplo de uso:
//
//	 Asumiendo que 'myDB' es una instancia *bolt.DB abierta
//	 tradesBucket, err := initBucket(myDB, "trades_data")
//	 if err != nil {
//	 	log.Fatalf("Error al inicializar el bucket de trades: %v", err)
//	 }
//	 Ahora puedes usar 'tradesBucket' para almacenar datos:
//	tradesBucket.Put([]byte("timestamp"), []byte("some trade details"))
//
// .
func InitBucket(opt BkOptions) (*db.Bucket, error) {
	var bucket *db.Bucket // Declara una variable para almacenar el bucket fuera del scope de la transacción

	// db.Update ejecuta una transacción de lectura/escritura.
	// La función pasada como argumento recibe un *db.Tx (transacción).
	err := opt.DB_INSTANCE.Update(func(tx *db.Tx) error {
		var txErr error // Variable local para capturar el error de la creación del bucket
		// CreateBucketIfNotExists intenta obtener el bucket por su nombre.
		// Si no existe, lo crea. Retorna el bucket o un error.
		bucket, txErr = tx.CreateBucketIfNotExists([]byte(opt.BUCKET_NAME))
		return txErr // Retorna el error de la transacción al manejador de db.Update
	})

	// Si db.Update devuelve un error (indicando que la transacción falló),
	// devolvemos ese error.
	if err != nil {
		//
		// NO HACER DESTRUYE LA DB Y FALLA EL PUNTERO
		// defer dbInstance.Close()
		//
		return nil, err
	}

	// Si la transacción fue exitosa, 'bucket' contendrá el puntero al *bolt.Bucket
	// y 'err' será nil. Se retornan ambos.
	return bucket, nil // El 'err' aquí será nil si la transacción fue exitosa
}

// initBucketWithRetries intenta obtener o crear un bucket dentro de una base de datos bbolt,
// aplicando una estrategia de reintentos con retroceso exponencial.
//
// Esta función es útil para manejar condiciones transitorias que podrían impedir
// el acceso o la creación del bucket, como bloqueos temporales del archivo de la DB,
// picos de carga en el sistema de almacenamiento, o problemas de concurrencia al inicio.
//
// Parámetros:
//   - cfg: Una estructura 'bkOptions' que debe contener la instancia de la base de datos
//     bbolt ('dbInstance *bolt.DB') y el nombre del bucket deseado ('bucketName string').
//
// Devuelve:
//   - Un puntero a '*bolt.Bucket' y 'nil' si el bucket se obtiene o se crea exitosamente
//     después de uno o varios intentos.
//   - 'nil' y un 'error' si se agotan todos los reintentos, si ocurre un fallo permanente
//     en la operación, o si el resultado obtenido no es del tipo esperado '*bolt.Bucket'.
//
// Configuración de reintentos interna:
//   - Máximo de n reintentos.
//   - Retraso inicial (backoff) de n milisegundos.
//   - Retraso máximo (maxBackoff) de n segundos.
func InitBucketWithRetries(cfg BkOptions) (*db.Bucket, error) {
	// Llama a executeActionWithRetries para orquestar los reintentos.
	// La función de acción anónima intenta inicializar el bucket usando initBucket.
	rawResponse, err := executeActionWithRetries(
		func(attempt int) (interface{}, error) {
			// Intenta obtener o crear el bucket utilizando la instancia de DB y el nombre del bucket
			// proporcionados en la configuración 'cfg'.
			bkInstance, bkErr := InitBucket(cfg)
			return bkInstance, bkErr // Devuelve la instancia del bucket y el error
		},
		func(err error, msg string) {
			// La función de manejo de errores se llama si un intento falla.
			// Proporciona detalles del error a handleErrorLogIt.
			handleErrorLogIt(err, msg)
		},
		5,                    // Número máximo de reintentos
		500*time.Millisecond, // Backoff inicial
		5*time.Second,        // Backoff máximo
		"apertura bbolt db",  // Nombre descriptivo de la acción para fines de logging
	)

	// Si 'executeActionWithRetries' devuelve un error, significa que todos los reintentos fallaron.
	if err != nil {
		return nil, fmt.Errorf("fallo definitivo al abrir la base de datos BBolt: %w", err)
	}

	// Realiza un type assertion para convertir el 'interface{}' de 'rawResponse' a '*bolt.Bucket'.
	// Esto es necesario porque executeActionWithRetries es genérica y devuelve 'interface{}'.
	bkInstance, ok := rawResponse.(*db.Bucket)
	if !ok {
		//
		// NO HACER DESTRUYE LA DB Y FALLA EL PUNTERO
		// defer cfg.dbInstance.Close()
		//
		// Retorna un error si el resultado de la acción no es del tipo esperado,
		// lo que indica un problema lógico o un tipo de retorno inesperado.
		return nil, fmt.Errorf("resultado inesperado: se esperaba *db.Bucket pero se obtuvo otro tipo")
	}

	// Si la operación fue exitosa después de los reintentos y el tipo es correcto,
	// devuelve la instancia del bucket.
	return bkInstance, nil
}

// SaveQuotesConcurrently procesa y guarda un slice de Quotes en la DB de forma concurrente.
// Utiliza un pool de workers para limitar la concurrencia y procesar en lotes.
func SaveQuotesConcurrently(
	dbInstance *db.DB,
	symbol string,
	quotes []oneQuote, // Asumo que Quote es el tipo de cada elemento en thisQuote.Quotes
	batchSize int, // Número de quotes a procesar en cada lote/transacción
	numWorkers int, // Número de goroutines concurrentes
) error {
	if dbInstance == nil {
		return fmt.Errorf("instancia de base de datos nula")
	}
	if len(quotes) == 0 {
		log.Println("No quotes to save.")
		return nil
	}

	// Channel para enviar quotes a los workers
	quotesChan := make(chan oneQuote, numWorkers*2) // Un poco de buffer
	// WaitGroup para esperar a que todos los workers terminen
	var wg sync.WaitGroup
	// Channel para recoger errores de los workers
	errChan := make(chan error, numWorkers)

	// Nombres de los sub-buckets para cada campo
	// Deberías usar constantes aquí, por ejemplo:
	// const (
	//     AP_BUCKET = "AP"
	//     AS_BUCKET = "AS"
	//     // ... etc
	// )
	fieldBuckets := []string{"AP", "AS", "AX", "BP", "BS", "BX", "C", "Z"} // No incluyes 'T' porque es la clave

	// Lanzar workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localBatch := make([]oneQuote, 0, batchSize) // Buffer local para acumular quotes por worker

			for quote := range quotesChan {
				localBatch = append(localBatch, quote)
				if len(localBatch) >= batchSize {
					if err := processAndSaveBatch(dbInstance, symbol, localBatch, fieldBuckets); err != nil {
						errChan <- fmt.Errorf("worker %d failed to save batch: %w", workerID, err)
						return // Sale del worker si hay un error fatal
					}
					localBatch = make([]oneQuote, 0, batchSize) // Reinicia el lote
				}
			}
			// Guardar cualquier quote restante en el lote final del worker
			if len(localBatch) > 0 {
				if err := processAndSaveBatch(dbInstance, symbol, localBatch, fieldBuckets); err != nil {
					errChan <- fmt.Errorf("worker %d failed to save final batch: %w", workerID, err)
				}
			}
		}(i)
	}

	// Enviar todas las quotes al channel de entrada
	for _, q := range quotes {
		quotesChan <- q
	}
	close(quotesChan) // Cierra el channel de jobs para indicar que no hay más trabajos

	// Esperar a que todos los workers terminen
	wg.Wait()
	close(errChan) // Cierra el channel de errores después de que todos los workers hayan terminado

	// Comprobar si hubo errores
	for err := range errChan {
		return fmt.Errorf("uno o más workers fallaron: %w", err)
	}

	return nil
}

// processAndSaveBatch procesa un lote de quotes y las guarda en bbolt en una única transacción.
// Utiliza el timestamp de la quote como clave binaria (Unix Nano).
func processAndSaveBatch(dbInstance *db.DB, symbol string, quotes []oneQuote, fieldBuckets []string) error {
	return dbInstance.Update(func(tx *db.Tx) error {
		// Obtener o crear el bucket principal del símbolo
		symbolBucket, err := tx.CreateBucketIfNotExists([]byte(symbol))
		if err != nil {
			return fmt.Errorf("failed to create symbol bucket '%s': %w", symbol, err)
		}

		// Crear/obtener sub-buckets para cada campo si no existen
		subBuckets := make(map[string]*db.Bucket)
		for _, field := range fieldBuckets {
			subB, err := symbolBucket.CreateBucketIfNotExists([]byte(field))
			if err != nil {
				return fmt.Errorf("failed to create sub-bucket '%s' for symbol '%s': %w", field, symbol, err)
			}
			subBuckets[field] = subB
		}

		// Iterar sobre cada quote en el lote
		for _, q := range quotes {
			// --- INICIO: Procesamiento del Timestamp a Unix Nano Key ---
			timestampStr := q.T // Asumo que q.T es un string (e.g., "2023-10-27T09:30:00.123456789Z")

			// Parsear el timestamp string a time.Time
			t, err := time.Parse(time.RFC3339Nano, timestampStr)
			if err != nil {
				// Es crucial manejar este error. Puedes loggearlo y saltar la quote
				// o retornar el error para abortar el lote. Depende de tu política de errores.
				log.Printf("Warning: Error parsing timestamp '%s': %v. Skipping this quote.", timestampStr, err)
				continue // Saltar esta quote si el timestamp no es válido
			}

			// Convertir time.Time a Unix Nanoseconds (int64)
			unixNano := t.UnixNano()

			// Convertir el int64 Unix Nano a un []byte de 8 bytes (Big Endian)
			// Esto asegura que la clave sea de tamaño fijo y se ordene correctamente.
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(unixNano)) // Siempre cast a uint64 para PutUint64
			// --- FIN: Procesamiento del Timestamp a Unix Nano Key ---

			// Ahora, guarda cada campo en su sub-bucket correspondiente usando 'key'
			// Asegúrate de que los valores sean convertidos a []byte.
			// Para números, usa strconv para evitar la asignación de memoria extra de fmt.Sprintf.

			// Ask Price (AP)
			apBytes := strconv.FormatFloat(q.AP, 'f', -1, 64) // 'f' formato, -1 para menor precisión, 64 bits
			if err := subBuckets["AP"].Put(key, []byte(apBytes)); err != nil {
				return fmt.Errorf("failed to put AP for %s: %w", string(key), err)
			}

			// Ask Size (AS)
			asBytes := strconv.Itoa(q.AS)
			if err := subBuckets["AS"].Put(key, []byte(asBytes)); err != nil {
				return fmt.Errorf("failed to put AS for %s: %w", string(key), err)
			}

			// Ask Exchange (AX)
			if err := subBuckets["AX"].Put(key, []byte(q.AX)); err != nil {
				return fmt.Errorf("failed to put AX for %s: %w", string(key), err)
			}

			// Repite para BP, BS, BX
			bpBytes := strconv.FormatFloat(q.BP, 'f', -1, 64)
			if err := subBuckets["BP"].Put(key, []byte(bpBytes)); err != nil {
				return fmt.Errorf("failed to put BP for %s: %w", string(key), err)
			}
			bsBytes := strconv.Itoa(q.BS)
			if err := subBuckets["BS"].Put(key, []byte(bsBytes)); err != nil {
				return fmt.Errorf("failed to put BS for %s: %w", string(key), err)
			}
			if err := subBuckets["BX"].Put(key, []byte(q.BX)); err != nil {
				return fmt.Errorf("failed to put BX for %s: %w", string(key), err)
			}

			// Para Conditions (C) - slice de strings, serializar a JSON
			cBytes, err := json.Marshal(q.C)
			if err != nil {
				return fmt.Errorf("failed to marshal conditions for %s: %w", string(key), err)
			}
			if err := subBuckets["C"].Put(key, cBytes); err != nil {
				return fmt.Errorf("failed to put C for %s: %w", string(key), err)
			}

			// Para Tape (Z)
			if err := subBuckets["Z"].Put(key, []byte(q.Z)); err != nil {
				return fmt.Errorf("failed to put Z for %s: %w", string(key), err)
			}
		}
		return nil // Transacción exitosa para este lote
	})
}
