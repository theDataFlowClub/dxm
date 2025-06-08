package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
)

// manejo de error -- log
func handleErrorLogIt(err error, message string) {
	log.Printf("%s: %v", message, err)
	// Aquí iría tu lógica de notificación/marcaje/reprocesamiento
	// Aquí podrías:
	// - Notificar vía Slack/Telegram.
	// - Marcar el proceso como "pausado" para ese símbolo.
	// - Escribir a un log de errores críticos para reprocesamiento.
}

/*
WEB QUERY ADDRESS - BUILD IT (GCI QUERY)
// =========================================
*/
type WebQueryAddress struct {
	protocol string
	domain   string
	path     string
	query    string
}

func webQuery(data WebQueryAddress) string {
	if data.protocol == "" {
		data.protocol = "https"
	}
	fullAddress := data.protocol + "://" + data.domain + data.path + "?" + data.query
	//fmt.Println(fullAddress) // Output: https://example.com/api/data?param1=value1&param2=value2

	return fullAddress
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

/*
WEB QUERY ADDRESS
// =========================================
Funcion de reintento con retroceso exponencial
// =
es fundamental en casi cualquier escenario donde interactúas con
servicios externos o componentes remotos:

Comunicación con APIs Externas (como Alpaca):

Rate Limiting (Límites de Tasa): Las APIs suelen imponer límites de cuántas peticiones puedes hacer por minuto/segundo. El retroceso exponencial, especialmente con jitter, ayuda a distribuir tus reintentos y evitar exceder estos límites cuando el servicio está ocupado.
Fallos de Red: Micro-cortes de red, congestión, DNS temporalmente no disponible.
Errores del Servidor Remoto (5xx): El servidor puede estar reiniciando, bajo alta carga, o experimentando un fallo temporal. Códigos como 500 Internal Server Error, 503 Service Unavailable, 504 Gateway Timeout son candidatos perfectos para reintentos.
Errores de Autenticación/Autorización (4xx): ¡Cuidado aquí! Generalmente no se reintenta un error 401 (Unauthorized) o 403 (Forbidden) con backoff, porque indica un problema de credenciales o permisos que no se resolverá con el tiempo. Sin embargo, un 429 Too Many Requests (indicando throttling) sí es un buen candidato para reintentar con backoff. Es crucial distinguir entre errores transitorios y permanentes.
Operaciones de Base de Datos (SQL, NoSQL, bbolt):

Contención de Bloqueos: Si múltiples transacciones intentan escribir a la misma fila/documento/clave, pueden producirse bloqueos o errores de concurrencia. Un reintento con backoff permite que la transacción compita de nuevo después de que el bloqueo se haya liberado.
Timeouts de Conexión: La base de datos puede estar momentáneamente inaccesible o saturada.
Fallos Temporales del Servidor de DB: Reinicios, parches, fallos de hardware menores.
Deadlocks (Interbloqueos): En bases de datos relacionales, los deadlocks pueden resolverse reintentando una de las transacciones implicadas.
Sistemas de Mensajería (Kafka, RabbitMQ, SQS):

Cuando un consumidor falla al procesar un mensaje (debido a un error de lógica, dependencia externa caída, etc.), a menudo el mensaje se pone en una cola de "reintentos" o se vuelve a encolar con un retraso, usando un mecanismo de backoff.
Publicar mensajes cuando el broker está saturado.
Sistemas de Archivos Remotos / Almacenamiento en la Nube (S3, GCS, Azure Blob Storage):

Lecturas o escrituras de archivos que pueden fallar debido a congestión de red, problemas en el servidor de almacenamiento o micro-cortes.
Servicios de Descubrimiento / Balanceadores de Carga:

Cuando un cliente intenta conectarse a un servicio a través de un servicio de descubrimiento o un balanceador de carga, y el servicio objetivo está momentáneamente no disponible o sobrecargado, el reintento con backoff es clave.
Cachés Distribuidas (Redis, Memcached):

Operaciones de lectura o escritura que fallan debido a problemas de red o sobrecarga del servidor de caché.
*/

// Define el tipo de la función de acción principal (ActionA)
// Toma un 'attempt' (int) y devuelve un 'error'.
// Nueva firma de ActionFunc: ahora puede devolver un resultado (interface{}) o un error
type ActionFunc func(attempt int) (interface{}, error)

// Define el tipo de la función de manejo de errores (ActionB)
// Toma el 'error' del intento fallido.
// Ajustada para el mensaje
type ErrorHandlerFunc func(err error, message string)

// executeActionWithRetries intenta ejecutar una acción con reintentos y retroceso exponencial.
// Toma la función de acción a ejecutar y una función de manejo de errores en caso de fallo. devuelve (interface{}, error)
func executeActionWithRetries(
	action ActionFunc,
	errorHandler ErrorHandlerFunc,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	actionName string, // Un nombre para la acción para mejor logging
) (interface{}, error) { // <-- ¡CAMBIO CLAVE AQUÍ!
	rand.Seed(time.Now().UnixNano())

	var result interface{} // Variable para almacenar el resultado exitoso

	for i := 1; i <= maxRetries; i++ {
		log.Printf("Intentando '%s' (Intento %d)...", actionName, i)
		res, err := action(i) // Ejecutar la acción
		if err == nil {
			log.Printf("Acción '%s' completada exitosamente en intento %d.", actionName, i)
			return res, nil // Devolver el resultado y nil error
		}

		// Acción falló, ejecutar el manejador de errores
		errorHandler(err, fmt.Sprintf("Fallo en '%s'", actionName))

		if i < maxRetries {
			backoff := initialBackoff * time.Duration(1<<uint(i-1))
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			sleepDuration := backoff + jitter

			log.Printf("Esperando %v antes del próximo reintento de '%s' (Intento %d/%d)...", sleepDuration, actionName, i+1, maxRetries)
			time.Sleep(sleepDuration)
		} else {
			log.Printf("executeActionWithRetries: Se agotaron los reintentos para '%s' después de %d intentos. Fallo definitivo.", actionName, maxRetries)
			return nil, fmt.Errorf("fallo definitivo de '%s' después de %d reintentos: %w", actionName, maxRetries, err)
		}
	}
	return result, nil // Debería ser unreachable si se llega al último intento, pero Go requiere un retorno aquí
}
