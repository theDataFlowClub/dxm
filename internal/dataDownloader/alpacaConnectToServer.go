package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

/*
//
//
//

BLOQUE DE alpacaCallItWithRetries
// ===============================

//
//
//
*/
// alpacaCallItOptions contiene las opciones de configuración para una llamada con reintentos a la API de Alpaca.
type AlpacaCallItOptions struct {
	URL             string        // URL completa con parámetros para la petición
	MAX_RETRIES     int           // Número máximo de reintentos permitidos
	MAX_BACKOFF     time.Duration // Tiempo máximo de espera entre reintentos
	INITIAL_BACKOFF time.Duration // Tiempo inicial de espera entre reintentos
	LOG_TEXT        string        // Texto a usar para logging de esta acción
}

// callIt realiza una petición HTTP GET simple a la URL indicada, incluyendo
// los encabezados de autenticación requeridos por la API de Alpaca.
//
// Esta función es la capa base para las comunicaciones con la API de Alpaca.
// No incorpora lógica de reintentos; se espera que las funciones de nivel superior
// (como `alpacaCallItWithRetries`) manejen los reintentos y el retroceso exponencial
// en caso de fallos transitorios.
//
// Parámetros:
//   - url: La URL completa del endpoint de la API de Alpaca al que se desea llamar
//     (ej., "[https://data.alpaca.markets/v2/stocks/AAPL/quotes](https://data.alpaca.markets/v2/stocks/AAPL/quotes)").
//
// Devuelve:
//   - Una cadena (string) que contiene el cuerpo de la respuesta HTTP si la petición
//     es exitosa (normalmente un JSON).
//   - Un error si ocurre algún problema durante la creación de la petición,
//     la carga de la configuración, la ejecución de la petición HTTP, o la lectura
//     del cuerpo de la respuesta.
//
// Errores comunes que puede retornar:
//   - Si falla la carga de la configuración (terminará el programa con log.Fatalf).
//   - Si ocurre un error de red o de conexión durante la petición HTTP.
//   - Si hay un problema al leer el cuerpo de la respuesta HTTP.
//
// Nota: La autenticación se realiza añadiendo los encabezados "APCA-API-KEY-ID"
// y "APCA-API-SECRET-KEY" con los valores obtenidos de la configuración de la aplicación.
func CallIt(url string) (string, error) {

	// Crea una nueva petición HTTP GET. El tercer argumento (nil) es para el cuerpo de la petición.
	// El error devuelto por http.NewRequest se ignora aquí, asumiendo que la URL es válida.
	req, _ := http.NewRequest("GET", url, nil)

	// Carga la configuración de la aplicación para obtener las claves de la API.
	// Si hay un error al cargar la configuración, el programa termina aquí.
	appConfig, err := LoadConfigs()
	if err != nil {
		log.Fatalf("Error al cargar configuración: %v", err)
	}

	// Agrega los encabezados HTTP necesarios para la autenticación y el formato de respuesta.
	req.Header.Add("accept", "application/json")                     // Solicita una respuesta en formato JSON
	req.Header.Add("APCA-API-KEY-ID", appConfig.AlpacaAPIKey)        // Agrega la clave de API
	req.Header.Add("APCA-API-SECRET-KEY", appConfig.AlpacaSecretKey) // Agrega la clave secreta

	// Ejecuta la petición HTTP utilizando el cliente HTTP por defecto de Go.
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		// Si ocurre un error durante la petición (ej. problema de red), se devuelve un error.
		return "", fmt.Errorf("error making HTTP request: %v", err)
	}
	// Asegura que el cuerpo de la respuesta se cierre después de que la función termine,
	// para liberar recursos de red.
	defer res.Body.Close()

	// Lee todo el contenido del cuerpo de la respuesta HTTP.
	body, err := io.ReadAll(res.Body)
	if err != nil {
		// Si hay un error al leer el cuerpo de la respuesta, se devuelve un error.
		// Se utiliza %w para envolver el error original.
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	// Convierte el cuerpo de la respuesta (que es un slice de bytes) a una cadena y lo devuelve.
	return string(body), nil
}

// alpacaCallItWithRetries orquesta una llamada a la API de Alpaca con estrategia de reintentos y backoff exponencial.
//
// Esta función es útil para asegurar que las llamadas HTTP hacia servicios externos como Alpaca se realicen
// de forma resiliente, manejando errores transitorios como:
// - Fallos de red intermitentes
// - Respuestas 5xx del servidor
// - Limitaciones de tasa (rate limiting, ej. 429 Too Many Requests)
//
// Parámetros:
//   - opt: una estructura de tipo `alpacaCallItOptions` que contiene:
//   - opt.url: URL que se debe consultar (string)
//   - opt.MaxRetries: cantidad máxima de intentos antes de rendirse
//   - opt.initialBackoff: duración del primer intervalo de espera
//   - opt.maxBackoff: duración máxima entre reintentos
//   - opt.logText: nombre descriptivo de la acción para logging
//
// La función envuelve la llamada base `callIt(url string) (string, error)` dentro del mecanismo
// de reintentos definido por `executeActionWithRetries`. Captura y maneja errores, y asegura
// que el resultado final sea una cadena válida.
//
// Devuelve:
//   - (string, nil) en caso de éxito
//   - ("", error) si se agotan los reintentos o el resultado no es del tipo esperado
func AlpacaCallItWithRetries(opt AlpacaCallItOptions) (string, error) {
	// Paso 1: Ejecutar la llamada con reintentos
	rawResponse, err := ExecuteActionWithRetries(
		func(attempt int) (interface{}, error) {
			// Lógica real de la llamada
			res, callErr := CallIt(opt.URL)
			return res, callErr
		},
		func(err error, msg string) {
			// Manejador de errores con logging
			HandleErrorLogIt(err, msg)
		},
		opt.MAX_RETRIES,
		opt.INITIAL_BACKOFF,
		opt.MAX_BACKOFF,
		opt.LOG_TEXT,
	)

	// Paso 2: Si todos los reintentos fallan, devolver error final
	if err != nil {
		return "", fmt.Errorf("fallo definitivo en la descarga de Alpaca: %w", err)
	}

	// Paso 3: Verificar que el resultado sea una cadena
	resString, ok := rawResponse.(string)
	if !ok {
		return "", fmt.Errorf("resultado inesperado de la acción de descarga: no es una cadena")
	}

	// Paso 4: Retornar resultado exitoso
	return resString, nil
}
