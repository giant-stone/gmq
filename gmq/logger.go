package gmq

// Logger supports logging at various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(args ...interface{})

	// Debugf logs a message at Debug level.
	Debugf(template string, args ...interface{})

	// Info logs a message at Info level.
	Info(args ...interface{})

	// Infof logs a message at Info level.
	Infof(template string, args ...interface{})

	// Warn logs a message at Warning level.
	Warn(args ...interface{})

	// Warnf logs a message at Warning level.
	Warnf(template string, args ...interface{})

	// Error logs a message at Error level.
	Error(args ...interface{})

	// Errorf logs a message at Error level.
	Errorf(template string, args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...interface{})

	// Fatalf logs a message at Fatal level.
	Fatalf(template string, args ...interface{})
}
