package main

import (
	"fmt"
	"os"
	"time"

	db "go.etcd.io/bbolt"
)

type Quote struct {
	// defining struct variables
	NextPageToken string     `json:"next_page_token"` // Cryptographic value.
	Quotes        []oneQuote `json:"quotes"`          // Quotes Body
	Symbol        string     `json:"symbol"`          // ticker
}

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
	PATH string

	// FileMode son los permisos del sistema de archivos que se aplicarán si la base de datos
	// se crea por primera vez. Se representa utilizando los permisos de archivo de Unix.
	// Por ejemplo:
	//   - 0600: El propietario del archivo tiene permisos de lectura y escritura;
	//           nadie más tiene acceso.
	//   - 0644: El propietario tiene lectura y escritura; el grupo y otros solo tienen lectura.
	// Es de tipo os.FileMode, que es un alias para uint32, y requiere la importación del paquete "os".
	FILE_MODE os.FileMode

	// BoltOpts es un puntero a una instancia de 'bbolt.Options'.
	// Contiene opciones avanzadas y específicas para la apertura de la base de datos bbolt,
	// como el Timeout para bloquear el archivo de la DB y si la base de datos se abrirá
	// en modo de solo lectura (ReadOnly).
	BOLT_OPTS *db.Options
}

// WriteConfig es una instancia predefinida de `DBOptions` configurada para abrir
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
	PATH:      "db/ticks.db",
	FILE_MODE: 0600,
	BOLT_OPTS: &db.Options{
		Timeout:  500 * time.Millisecond,
		ReadOnly: false,
	},
}

// Configuración para operaciones de lectura (solo lectura)
var RaedConfig = DBOptions{
	PATH: "db/ticks.db",
	// Los permisos de creación solo importan si el archivo no existe
	FILE_MODE: 0400, // Owner has read access only. Group and others have no access.
	BOLT_OPTS: &db.Options{
		Timeout:  500 * time.Millisecond,
		ReadOnly: true, // ¡Importante para solo lectura!
	},
}

// initDB inicializa y abre una conexión a la base de datos bbolt.
//
// El parámetro 'class' determina el modo de apertura de la base de datos:
//   - "write": Abre la base de datos en modo de lectura/escritura (usando WriteConfig).
//   - "read": Abre la base de datos en modo de solo lectura (usando RaedConfig).
//
// Devuelve un puntero a un 'bolt.DB' si la conexión es exitosa, junto con 'nil' como error.
// Si ocurre un error al abrir la base de datos o si la clase especificada no es válida,
// devuelve 'nil' y un 'error' descriptivo.
//
// Ejemplos de uso:
//
//	dbInstance, err := initDB(WriteConfig) // Abrir para escritura
//	dbInstance, err := initDB(RaedConfig)  // Abrir para solo lectura
func InitDB(cfg DBOptions) (*db.DB, error) {
	// Declara 'cfg' en el ámbito de la función initDB
	//var cfg DBOptions
	//
	switch cfg.FILE_MODE {
	case 0600:
		cfg = WriteConfig
	case 0400:
		cfg = RaedConfig
	default:
		// Manejo de error si 'FileMode' no es ni 0600 ni 0400
		return nil, fmt.Errorf("error de seguridad: %v. Modo no valido", cfg.FILE_MODE)
	}
	//
	// Abrimos db con el archivo de configuración elegido
	// 'cfg' ahora es accesible aquí
	database, err := db.Open(cfg.PATH, cfg.FILE_MODE, cfg.BOLT_OPTS)
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
func initDBWithRetries(cfg DBOptions) (*db.DB, error) {
	rawResponse, err := executeActionWithRetries(
		func(attempt int) (interface{}, error) {
			dbInstance, dbErr := InitDB(cfg)
			return dbInstance, dbErr
		},
		func(err error, msg string) {
			handleErrorLogIt(err, msg)
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
