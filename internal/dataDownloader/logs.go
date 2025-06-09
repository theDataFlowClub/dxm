package main

import "log"

func LogInit() {
	// 1. Configurar la salida de logs:
	// Configuramos el logger estándar para escribir en el buffer.
	// Esto NO IMPRIMIRÁ EN CONSOLA por defecto.
	log.SetOutput(&LogBuffer)

	// Opcional: Para ver los logs en la consola Y en el buffer (útil para depuración)
	// log.SetOutput(io.MultiWriter(os.Stderr, &LogBuffer))
	// Asegúrate de que el logger no tenga prefijo de fecha/hora si no quieres que lo agregue,
	// o déjalo si quieres la marca de tiempo.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds) // Opcional: añade microsegundos para mayor precisión
}

// manejo de error -- log

func handleErrorLogIt(err error, message string) {
	// El log ya se almacena en LogBuffer porque log.SetOutput(&LogBuffer) en LogInit()
	// Si quieres acceder al contenido actual del buffer, puedes usar LogBuffer.String()
	// Aquí iría tu lógica de notificación/marcaje/reprocesamiento
	// Aquí podrías:
	// - Notificar vía Slack/Telegram.
	// - Marcar el proceso como "pausado" para ese símbolo.
	// - Escribir a un log de errores críticos para reprocesamiento.
}
