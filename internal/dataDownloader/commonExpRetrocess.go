package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// ActionFunc define el tipo de una función que realiza una acción sujeta a reintentos.
// Recibe el número del intento actual (comenzando en 1) y retorna el resultado (`interface{}`) o un error.
// Si la acción es exitosa, debe retornar un resultado no nulo y `nil` como error.
type ActionFunc func(attempt int) (interface{}, error)

// ErrorHandlerFunc define el tipo de una función que maneja errores durante los reintentos.
// Recibe el error producido y un mensaje personalizado asociado con la acción.
type ErrorHandlerFunc func(err error, message string)

// executeActionWithRetries intenta ejecutar una acción con múltiples reintentos,
// aplicando retroceso exponencial con "jitter" aleatorio para suavizar las colisiones.
//
// Es útil para manejar errores transitorios al interactuar con servicios externos como:
//   - APIs (p. ej. errores 429, 500, 503)
//   - Bases de datos (p. ej. timeouts, bloqueos, deadlocks)
//   - Sistemas de mensajería (Kafka, SQS)
//   - Servicios de almacenamiento o red saturados
//
// Parámetros:
// - action: función que encapsula la operación a ejecutar con reintentos.
// - errorHandler: función llamada tras cada fallo para loguear o gestionar el error.
// - maxRetries: número máximo de intentos permitidos.
// - initialBackoff: duración inicial de espera antes de reintentar.
// - maxBackoff: duración máxima permitida entre reintentos.
// - actionName: etiqueta que identifica la acción (para logging).
//
// Devuelve:
// - El resultado (`interface{}`) retornado por la acción en caso de éxito.
// - Un error si se agotaron los reintentos sin éxito.
//
// Ejemplo de retroceso exponencial con jitter:
//
//	Intento 1: espera 100ms + jitter
//	Intento 2: espera 200ms + jitter
//	Intento 3: espera 400ms + jitter (hasta maxBackoff)
func executeActionWithRetries(
	action ActionFunc,
	errorHandler ErrorHandlerFunc,
	maxRetries int,
	initialBackoff time.Duration,
	maxBackoff time.Duration,
	actionName string,
) (interface{}, error) {
	rand.Seed(time.Now().UnixNano())

	var result interface{}

	for i := 1; i <= maxRetries; i++ {
		log.Printf("Intentando '%s' (Intento %d)...", actionName, i)

		res, err := action(i)
		if err == nil {
			log.Printf("Acción '%s' completada exitosamente en intento %d.", actionName, i)
			return res, nil
		}

		errorHandler(err, fmt.Sprintf("Fallo en '%s'", actionName))

		if i < maxRetries {
			// Retroceso exponencial con jitter aleatorio (50% del backoff)
			backoff := initialBackoff * time.Duration(1<<uint(i-1))
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			sleepDuration := backoff + jitter

			log.Printf("Esperando %v antes del próximo reintento de '%s' (Intento %d/%d)...",
				sleepDuration, actionName, i+1, maxRetries)
			time.Sleep(sleepDuration)
		} else {
			log.Printf("executeActionWithRetries: Se agotaron los reintentos para '%s' después de %d intentos. Fallo definitivo.",
				actionName, maxRetries)
			return nil, fmt.Errorf("fallo definitivo de '%s' después de %d reintentos: %w", actionName, maxRetries, err)
		}
	}

	// Este retorno es inalcanzable, pero necesario para cumplir con la firma de la función
	return result, nil
}
