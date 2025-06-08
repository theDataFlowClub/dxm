package main

import (
	"fmt"

	// Importamos el paquete de configuración

	db "go.etcd.io/bbolt"
)

func initDB(class string) (*db.DB, error) {
	// Declara 'cfg' en el ámbito de la función initDB
	var cfg DBOptions
	//
	// Asigna el valor a 'cfg' basado en el parámetro 'class'
	if class == "write" {
		cfg = writeConfig // Asignación, no declaración aquí
	} else if class == "read" { // Es buena idea ser explícito con "read"
		cfg = readConfig // Asignación, no declaración aquí
	} else {
		// Manejo de error si 'class' no es ni "write" ni "read"
		return nil, fmt.Errorf("clase de base de datos no válida: %s. Debe ser 'write' o 'read'", class)
	}
	//
	// Abrimos db con el archivo de configuración elegido
	// 'cfg' ahora es accesible aquí
	database, err := db.Open(cfg.Path, cfg.FileMode, cfg.BoltOpts)
	if err != nil {
		// En lugar de log.Fatal, devuelve el error para que la función que llama lo maneje.
		// log.Fatal termina el programa inmediatamente, lo cual no siempre es deseable.
		return nil, fmt.Errorf("error al abrir la base de datos bbolt: %w", err)
	}
	// Si logra abrir, entrega la instancia de la base de datos y nulo para el error
	return database, nil
}

func initBucket(dbInstance *db.DB, bucketName string) (*db.Bucket, error) {
	var bucket *db.Bucket
	err := dbInstance.Update(func(tx *db.Tx) error {
		var err error
		bucket, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, err
	}
	return bucket, err
}
