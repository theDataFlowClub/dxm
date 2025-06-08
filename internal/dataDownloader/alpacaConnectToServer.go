package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// --- Función que basica de llamada a Alpaca ---
func callIt(url string) (string, error) {

	req, _ := http.NewRequest("GET", url, nil)

	appConfig, err := loadConfigs()
	if err != nil {
		log.Fatalf("Error al cargar configuración: %v", err)
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("APCA-API-KEY-ID", appConfig.AlpacaAPIKey)
	req.Header.Add("APCA-API-SECRET-KEY", appConfig.AlpacaSecretKey)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making HTTP request: %v", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	return string(body), nil

}

// --- Función que orquesta la llamada a Alpaca y el parsing ---
type alpacaCallItOptions struct {
	url            string
	MaxRetries     int
	maxBackoff     time.Duration
	initialBackoff time.Duration
	logText        string // Para logging, si quieres que la acción específica sea visible
	// Otros parámetros específicos de la API de Alpaca, si se vuelven fijos
	// StartTime string // Si no lo pasas dinámicamente en la URL
	// Limit int
}

func alpacaCallItWithRetries(opt alpacaCallItOptions) (string, error) {
	// Paso 1: Hacer la llamada a Alpaca con reintentos
	rawResponse, err := executeActionWithRetries(
		func(attempt int) (interface{}, error) { // ActionFunc que devuelve (string, error)
			res, callErr := callIt(opt.url)
			return res, callErr // Devolvemos el string y el error
		},
		func(err error, msg string) { handleErrorLogIt(err, msg) },
		opt.MaxRetries,     // maxRetries
		opt.initialBackoff, // initialBackoff
		opt.maxBackoff,     //2*time.Second,        // maxBackoff
		opt.logText,        // Nombre de la acción para el log
	)
	if err != nil {
		// El error definitivo ya incluye el mensaje de "fallo definitivo"
		return "", fmt.Errorf("fallo definitivo en la descarga de Alpaca: %w", err)
	}
	// Como rawResponse es interface{}, necesitamos hacer un type assertion
	resString, ok := rawResponse.(string)
	if !ok {
		return "", fmt.Errorf("resultado inesperado de la acción de descarga: no es una cadena")
	}
	// Ahora puedes retornar la cadena de respuesta exitosa
	return resString, nil
}
