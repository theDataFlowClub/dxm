package main

import (
	db "go.etcd.io/bbolt"
)

// Define and initialize thisDB as needed; here it's set to nil as a placeholder.
// Replace 'nil' with the actual *db.DB instance as appropriate in your application.
//
// thisDB es una variable global que representa la instancia principal de la base de datos bbolt
// utilizada por la aplicación.
// Se inicializa a `nil` como un marcador de posición y debe ser asignada con
// una instancia de `*db.DB` (por ejemplo, a través de `initDB` o `initDBWithRetries`)
// antes de ser utilizada para cualquier operación de base de datos.
var thisDB *db.DB = nil
