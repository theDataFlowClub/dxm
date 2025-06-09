package dataDownloader

import (
	"fmt"
	"log"
	"os"

	env "github.com/joho/godotenv"
)

// Configs struct para agrupar las configuraciones
type AppConfig struct {
	AlpacaAPIKey    string
	AlpacaSecretKey string
	DBPath          string // Si decides incluirlo
}

func LoadConfigs() (AppConfig, error) { // Modificado para devolver AppConfig y un error
	// Carga variables de entorno desde .env. Si no se encuentra, ignora el error
	err := env.Load("/Users/davidochoacorrales/Documents/GitHub/dxm/configs/secret/.env") // Ruta relativa desde la raíz de ejecución
	if err != nil {
		log.Printf("Advertencia: No se pudo cargar .env, asumiendo variables de entorno configuradas: %v", err)
	}

	config := AppConfig{
		AlpacaAPIKey:    os.Getenv("API_KEY_ID"),
		AlpacaSecretKey: os.Getenv("API_SECRET_KEY"),
		//DBPath:          os.Getenv("DB_PATH"),
	}

	if config.AlpacaAPIKey == "" || config.AlpacaSecretKey == "" {
		return AppConfig{}, fmt.Errorf("las claves api de alpaca no están configuradas en el entorno o en .env")
	}

	//fmt.Printf("API Key: %s...\n", config.AlpacaAPIKey[:5]) // Mostrar solo los primeros caracteres por seguridad
	//fmt.Printf("DB Path: %s\n", config.DBPath)

	return config, nil // Devolver la estructura de configuración y nil para el error
}
