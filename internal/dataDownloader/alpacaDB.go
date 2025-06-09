package dataDownloader

import (
	"fmt"
	"os"
	"time"

	db "go.etcd.io/bbolt"
)

/*
//
//
//

BLOQUE DE initDB
// ===============================

//
//
//
*/
// DBOptions es una estructura que encapsula las opciones de configuración necesarias
// para abrir una base de datos bbolt. Proporciona un conjunto claro de parámetros
// para controlar la ubicación del archivo, sus permisos y las propiedades de la conexión.
type DBOptions struct {
	// Path es la ruta completa al archivo de la base de datos bbolt en el sistema de archivos.
	// Ejemplos: "db/ticks.db", "/var/lib/mybot/data.db".
	Path string

	// FileMode son los permisos del sistema de archivos que se aplicarán si la base de datos
	// se crea por primera vez. Se representa utilizando los permisos de archivo de Unix.
	// Por ejemplo:
	//   - 0600: El propietario del archivo tiene permisos de lectura y escritura;
	//           nadie más tiene acceso.
	//   - 0644: El propietario tiene lectura y escritura; el grupo y otros solo tienen lectura.
	// Es de tipo os.FileMode, que es un alias para uint32, y requiere la importación del paquete "os".
	FileMode os.FileMode

	// BoltOpts es un puntero a una instancia de 'bbolt.Options'.
	// Contiene opciones avanzadas y específicas para la apertura de la base de datos bbolt,
	// como el Timeout para bloquear el archivo de la DB y si la base de datos se abrirá
	// en modo de solo lectura (ReadOnly).
	BoltOpts *db.Options
}

// writeConfig es una instancia predefinida de `DBOptions` configurada para abrir
// una base de datos bbolt en modo de lectura/escritura.
// Es adecuada para operaciones que modifican los datos de la base de datos.
//
// Propiedades:
//   - Path: "db/ticks.db" - Ruta predeterminada del archivo de la base de datos.
//   - FileMode: 0600 - Permite solo al propietario leer y escribir el archivo.
//   - BoltOpts:
//   - Timeout: 500 * time.Millisecond - Tiempo máximo de espera para obtener un bloqueo
//     exclusivo sobre el archivo de la base de datos.
//   - ReadOnly: false - Indica que la base de datos se abrirá en modo de lectura y escritura.
var WriteConfig = DBOptions{
	Path:     "db/ticks.db",
	FileMode: 0600,
	BoltOpts: &db.Options{
		Timeout:  500 * time.Millisecond,
		ReadOnly: false,
	},
}

// Configuración para operaciones de lectura (solo lectura)
var ReadConfig = DBOptions{
	Path: "db/ticks.db",
	// Los permisos de creación solo importan si el archivo no existe
	FileMode: 0400, // Owner has read access only. Group and others have no access.
	BoltOpts: &db.Options{
		Timeout:  500 * time.Millisecond,
		ReadOnly: true, // ¡Importante para solo lectura!
	},
}

// initDB inicializa y abre una conexión a la base de datos bbolt.
//
// El parámetro 'class' determina el modo de apertura de la base de datos:
//   - "write": Abre la base de datos en modo de lectura/escritura (usando writeConfig).
//   - "read": Abre la base de datos en modo de solo lectura (usando readConfig).
//
// Devuelve un puntero a un 'bolt.DB' si la conexión es exitosa, junto con 'nil' como error.
// Si ocurre un error al abrir la base de datos o si la clase especificada no es válida,
// devuelve 'nil' y un 'error' descriptivo.
//
// Ejemplos de uso:
//
//	dbInstance, err := initDB(writeConfig) // Abrir para escritura
//	dbInstance, err := initDB(readConfig)  // Abrir para solo lectura
func InitDB(cfg DBOptions) (*db.DB, error) {
	// Declara 'cfg' en el ámbito de la función initDB
	//var cfg DBOptions
	//
	switch cfg.FileMode {
	case 0600:
		cfg = WriteConfig
	case 0400:
		cfg = ReadConfig
	default:
		// Manejo de error si 'FileMode' no es ni 0600 ni 0400
		return nil, fmt.Errorf("error de seguridad: %v. Modo no valido", cfg.FileMode)
	}
	//
	// Abrimos db con el archivo de configuración elegido
	// 'cfg' ahora es accesible aquí
	database, err := db.Open(cfg.Path, cfg.FileMode, cfg.BoltOpts)
	if err != nil {
		//
		// NO HACER DESTRUYE LA DB Y FALLA EL PUNTERO
		// defer database.Close()
		//
		// En lugar de log.Fatal, devuelve el error para que la función que llama lo maneje.
		// log.Fatal termina el programa inmediatamente, lo cual no siempre es deseable.
		return nil, fmt.Errorf("error al abrir la base de datos bbolt: %w", err)
	}
	// Si logra abrir, entrega la instancia de la base de datos y nulo para el error
	return database, nil
}

// initDBWithRetries intenta abrir una base de datos BBolt utilizando una estrategia de reintentos
// con retroceso exponencial.
//
// Esta función es ideal para manejar fallos temporales o condiciones transitorias al intentar
// acceder o abrir el archivo de la base de datos, como bloqueos temporales del sistema de archivos,
// contención de IO, o durante el inicio de la aplicación en entornos donde los recursos
// pueden no estar inmediatamente disponibles.
//
// Parámetros:
//   - cfg: Una estructura `DBOptions` que contiene la configuración necesaria para la apertura
//     de la base de datos (ruta, permisos y opciones específicas de bbolt).
//
// Devuelve:
//   - Un puntero a `*bolt.DB` y `nil` si la base de datos se abre exitosamente después de
//     uno o varios intentos.
//   - `nil` y un `error` si se agotan todos los reintentos o si el resultado obtenido
//     no es del tipo esperado `*bolt.DB`.
//
// Configuración de reintentos interna:
//   - Máximo de N reintentos.
//   - Retraso inicial (backoff) de N milisegundos.
//   - Retraso máximo (maxBackoff) de N segundos.
func InitDBWithRetries(cfg DBOptions) (*db.DB, error) {
	rawResponse, err := ExecuteActionWithRetries(
		func(attempt int) (interface{}, error) {
			dbInstance, dbErr := InitDB(cfg)
			return dbInstance, dbErr
		},
		func(err error, msg string) {
			HandleErrorLogIt(err, msg)
		},
		5,                    // Número máximo de reintentos
		500*time.Millisecond, // Backoff inicial
		5*time.Second,        // Backoff máximo
		"apertura bbolt db",
	)

	if err != nil {
		return nil, fmt.Errorf("fallo definitivo al abrir la base de datos BBolt: %w", err)
	}

	dbInstance, ok := rawResponse.(*db.DB)
	if !ok {
		//
		// NO HACER DESTRUYE LA DB Y FALLA EL PUNTERO
		// defer dbInstance.Close()
		//
		return nil, fmt.Errorf("resultado inesperado: se esperaba *db.DB pero se obtuvo otro tipo")
	}

	return dbInstance, nil
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
// alpacaCallItOptions contiene las opciones de configuración para un
// quote optenido de alpaca
type AlpacaQuoteBuketSlots struct {
	ap string
	as string
	ax string
	bp string
	bs string
	bx string
	c  string
	z  string
	t  string
}

// bkOptions es una estructura que agrupa los parámetros necesarios para
// identificar y operar sobre un bucket específico en una base de datos bbolt.
// Se utiliza típicamente para pasar estos parámetros a funciones que interactúan
// con buckets, como `initBucketWithRetries`.
type BkOptions struct {
	// dbInstance es un puntero a la instancia de la base de datos bbolt
	// a la que pertenece el bucket.
	dbInstance *db.DB
	// bucketName es el nombre del bucket dentro de la base de datos,
	// representado como uSna cadena.
	bucketName string
	//
	alpacaQuoteBuketSlots AlpacaQuoteBuketSlots
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
	err := opt.dbInstance.Update(func(tx *db.Tx) error {
		var txErr error // Variable local para capturar el error de la creación del bucket
		// CreateBucketIfNotExists intenta obtener el bucket por su nombre.
		// Si no existe, lo crea. Retorna el bucket o un error.
		bucket, txErr = tx.CreateBucketIfNotExists([]byte(opt.bucketName))
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
	rawResponse, err := ExecuteActionWithRetries(
		func(attempt int) (interface{}, error) {
			// Intenta obtener o crear el bucket utilizando la instancia de DB y el nombre del bucket
			// proporcionados en la configuración 'cfg'.
			bkInstance, bkErr := InitBucket(cfg)
			return bkInstance, bkErr // Devuelve la instancia del bucket y el error
		},
		func(err error, msg string) {
			// La función de manejo de errores se llama si un intento falla.
			// Proporciona detalles del error a handleErrorLogIt.
			HandleErrorLogIt(err, msg)
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
