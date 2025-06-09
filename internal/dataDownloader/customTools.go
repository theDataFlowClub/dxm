package dataDownloader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"time"
)

var logBuffer bytes.Buffer // Un buffer en memoria para capturar los logs

func logInit() {
	// 1. Configurar la salida de logs:
	// Configuramos el logger estándar para escribir en el buffer.
	// Esto NO IMPRIMIRÁ EN CONSOLA por defecto.
	log.SetOutput(&logBuffer)

	// Opcional: Para ver los logs en la consola Y en el buffer (útil para depuración)
	// log.SetOutput(io.MultiWriter(os.Stderr, &logBuffer))
	// Asegúrate de que el logger no tenga prefijo de fecha/hora si no quieres que lo agregue,
	// o déjalo si quieres la marca de tiempo.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds) // Opcional: añade microsegundos para mayor precisión
}

// manejo de error -- log

func handleErrorLogIt(err error, message string) {
	// El log ya se almacena en logBuffer porque log.SetOutput(&logBuffer) en logInit()
	// Si quieres acceder al contenido actual del buffer, puedes usar logBuffer.String()
	// Aquí iría tu lógica de notificación/marcaje/reprocesamiento
	// Aquí podrías:
	// - Notificar vía Slack/Telegram.
	// - Marcar el proceso como "pausado" para ese símbolo.
	// - Escribir a un log de errores críticos para reprocesamiento.
}

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

// webQuery construye una URL completa a partir de los campos del struct WebQueryAddress.
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
func webQuery(data WebQueryAddress) string {
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
