package log

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger

func init() {
	// Configure zerolog for pretty console output with caller information
	output := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05",
		FormatFieldName: func(i interface{}) string {
			if fieldName, ok := i.(string); ok && fieldName == "caller" {
				return ""
			}
			return fmt.Sprintf("%s=", i)
		},
		FormatFieldValue: func(i interface{}) string {
			if fieldName, ok := i.(string); ok && fieldName == "caller" {
				return fmt.Sprintf("[%s]", i)
			}
			return fmt.Sprintf("%s", i)
		},
	}

	logger = zerolog.New(output).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()
}

// getCaller returns the file and line number of the actual caller
func getCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown:0"
	}

	// Extract just the filename from the full path
	parts := strings.Split(file, "/")
	filename := parts[len(parts)-1]

	return filename + ":" + strconv.Itoa(line)
}

// Info creates an info level log event with caller information
func Info() *zerolog.Event {
	return logger.Info().Str("caller", getCaller(2))
}

// Warn creates a warn level log event with caller information
func Warn() *zerolog.Event {
	return logger.Warn().Str("caller", getCaller(2))
}

// Error creates an error level log event with caller information
func Error() *zerolog.Event {
	return logger.Error().Str("caller", getCaller(2))
}

// Fatal creates a fatal level log event with caller information
func Fatal() *zerolog.Event {
	return logger.Fatal().Str("caller", getCaller(2))
}

// Debug creates a debug level log event with caller information
func Debug() *zerolog.Event {
	return logger.Debug().Str("caller", getCaller(2))
}

// SetLevel sets the global log level
func SetLevel(level zerolog.Level) {
	logger = logger.Level(level)
}

// SetDebug enables debug logging
func SetDebug() {
	SetLevel(zerolog.DebugLevel)
}

// Infof logs a formatted info message with caller information
func Infof(format string, args ...interface{}) {
	logger.Info().Str("caller", getCaller(2)).Msgf(format, args...)
}

// Warnf logs a formatted warn message with caller information
func Warnf(format string, args ...interface{}) {
	logger.Warn().Str("caller", getCaller(2)).Msgf(format, args...)
}

// Errorf logs a formatted error message with caller information
func Errorf(format string, args ...interface{}) {
	logger.Error().Str("caller", getCaller(2)).Msgf(format, args...)
}

// Fatalf logs a formatted fatal message with caller information
func Fatalf(format string, args ...interface{}) {
	logger.Fatal().Str("caller", getCaller(2)).Msgf(format, args...)
}

// Debugf logs a formatted debug message with caller information
func Debugf(format string, args ...interface{}) {
	logger.Debug().Str("caller", getCaller(2)).Msgf(format, args...)
}
