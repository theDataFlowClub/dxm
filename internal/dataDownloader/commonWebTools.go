package main

import (
	"encoding/json"
	"log"
	"net/url"
)

//
// WEB QUERY ADDRESS - BUILD IT (GCI QUERY)
// =========================================
//
// Este bloque permite construir una URL web completa a partir de sus componentes,
// incluyendo protocolo, dominio, ruta y cadena de consulta. Puede utilizarse
// para construir direcciones dinámicas hacia APIs u otros servicios.
//

// WebQueryAddress representa los componentes de una dirección web que será construida dinámicamente.
type WebQueryAddress struct {
	protocol string // Protocolo (http o https); si está vacío, se usa "https" por defecto
	domain   string // Dominio principal (ej. "example.com")
	path     string // Ruta del recurso (ej. "/api/data")
	query    string // Cadena de consulta (ej. "param1=value1&param2=value2")
}

// WebQuery construye una URL completa a partir de los campos del struct WebQueryAddress.
// en el struct WebQueryAddress. Utiliza el paquete estándar `net/url` para
// asegurar que la URL esté correctamente codificada.
//
// Si el campo `protocol` está vacío, se asigna automáticamente "https".
// Si la cadena `query` no puede ser parseada con `url.ParseQuery`,
// se utiliza directamente como fallback.
//
// Ejemplo de URL generada:
//
//	https://example.com/api/data?key1=value1&key2=value2
func WebQuery(data WebQueryAddress) string {
	// Establecer "https" como protocolo por defecto si no se especifica
	if data.protocol == "" {
		data.protocol = "https"
	}

	// Crear objeto URL con esquema, dominio y ruta
	u := url.URL{
		Scheme: data.protocol,
		Host:   data.domain,
		Path:   data.path,
	}

	// Intentar parsear la cadena de consulta
	queryParams, err := url.ParseQuery(data.query)
	if err != nil {
		// Si falla el parseo, mostrar advertencia en log y usar la query en crudo
		log.Printf("Error parsing query string: %v", err)
		u.RawQuery = data.query
	} else {
		// Si el parseo es exitoso, se codifican correctamente los parámetros
		u.RawQuery = queryParams.Encode()
	}

	// Retornar la URL como cadena
	return u.String()
}

/*
unmarshalGeneric

*/
// unmarshalGeneric toma los datos JSON como []byte y un puntero a la estructura destino.
// El destino (v) debe ser un puntero para que json.Unmarshal pueda modificarlo.
// La función devuelve un error si algo sale mal.
func unmarshalGeneric(data []byte, v interface{}) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return err
	}
	return nil
}
