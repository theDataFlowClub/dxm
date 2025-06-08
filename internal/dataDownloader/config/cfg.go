package config

import (
	"os"
	"time"

	db "go.etcd.io/bbolt"
)

// DBOptions es una estructura que encapsula las opciones directamente del paquete bbolt.
// Usamos "bbolt.Options" directamente para aprovechar las opciones ya definidas.
type DBOptions struct {
	// Path es la ruta al archivo de la base de datos.
	// Ej. "db/ticks.db"
	Path string

	// FileMode son los permisos de archivo para la base de datos recién creada.
	// Se usan los permisos de Unix, por ejemplo, 0600 para lectura y escritura
	// solo para el propietario, o 0644 para lectura y escritura del propietario
	// y solo lectura para otros.
	// Nota: En Go, los permisos de archivo se representan con os.FileMode (uint32),
	// por lo que usaremos ese tipo.
	FileMode os.FileMode // Importa "os" si no está ya en tu archivo principal

	// BoltOpts contiene las opciones específicas de bbolt.
	// La incrustación de una struct anónima es común si quieres "promover" sus campos,
	// pero aquí es más claro referenciarla explícitamente.
	BoltOpts *db.Options // Puntero a las opciones de bbolt
}

// Configuración para operaciones de escritura (lectura/escritura)
var writeConfig = DBOptions{
	Path:     "db/ticks.db",
	FileMode: 0600, // Permisos de archivo
	BoltOpts: &db.Options{
		Timeout:  500 * time.Millisecond,
		ReadOnly: false,
	},
}

// Configuración para operaciones de lectura (solo lectura)
var readConfig = DBOptions{
	Path:     "db/ticks.db",
	FileMode: 0600, // Los permisos de creación solo importan si el archivo no existe
	BoltOpts: &db.Options{
		Timeout:  500 * time.Millisecond,
		ReadOnly: true, // ¡Importante para solo lectura!
	},
}
