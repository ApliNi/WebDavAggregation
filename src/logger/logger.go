package logger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// Level represents the log severity level.
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

const (
	LevelDebug = DEBUG
	LevelInfo  = INFO
	LevelWarn  = WARN
	LevelError = ERROR
)

// String returns the uppercase string representation of the log level.
func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a case-insensitive string into a Level.
func ParseLevel(s string) (Level, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return DEBUG, nil
	case "INFO":
		return INFO, nil
	case "WARN", "WARNING":
		return WARN, nil
	case "ERROR":
		return ERROR, nil
	default:
		return INFO, fmt.Errorf("unknown log level: %q", s)
	}
}

// Logger formats and outputs logs with the format: [HH:MM:SS LEVEL]: [Type] msg
type Logger struct {
	mu    sync.RWMutex
	out   io.Writer
	level Level
}

var defaultLogger = New(os.Stdout, INFO)

// New creates a new Logger with the specified writer and minimum level.
func New(out io.Writer, level Level) *Logger {
	if out == nil {
		out = io.Discard
	}
	return &Logger{
		out:   out,
		level: level,
	}
}

// Default returns the default global Logger instance.
func Default() *Logger {
	return defaultLogger
}

// SetOutput updates the output destination for the logger.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w == nil {
		l.out = io.Discard
	} else {
		l.out = w
	}
}

// GetOutput returns the current output destination.
func (l *Logger) GetOutput() io.Writer {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.out
}

// SetLevel updates the minimum logging level.
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel returns the current minimum logging level.
func (l *Logger) GetLevel() Level {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

// IsLevelEnabled reports whether the given level will be logged.
func (l *Logger) IsLevelEnabled(level Level) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level >= l.level
}

// Log writes a formatted log entry if level satisfies the minimum threshold.
func (l *Logger) Log(level Level, typ string, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level < l.level || l.out == nil {
		return
	}

	if typ == "" {
		typ = "App"
	}

	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}

	msg = strings.ReplaceAll(msg, "\r\n", "\n")
	msg = strings.TrimRight(msg, "\n")

	timestamp := time.Now().Format("15:04:05")
	entry := fmt.Sprintf("[%s %s]: [%s] %s\n", timestamp, level.String(), typ, msg)
	_, _ = io.WriteString(l.out, entry)
}

// Logf writes a formatted log entry if level satisfies the minimum threshold.
func (l *Logger) Logf(level Level, typ string, format string, args ...any) {
	l.Log(level, typ, format, args...)
}

// Debug logs a message at DEBUG level.
func (l *Logger) Debug(typ string, format string, args ...any) {
	l.Log(DEBUG, typ, format, args...)
}

// Debugf logs a message at DEBUG level.
func (l *Logger) Debugf(typ string, format string, args ...any) {
	l.Log(DEBUG, typ, format, args...)
}

// Info logs a message at INFO level.
func (l *Logger) Info(typ string, format string, args ...any) {
	l.Log(INFO, typ, format, args...)
}

// Infof logs a message at INFO level.
func (l *Logger) Infof(typ string, format string, args ...any) {
	l.Log(INFO, typ, format, args...)
}

// Warn logs a message at WARN level.
func (l *Logger) Warn(typ string, format string, args ...any) {
	l.Log(WARN, typ, format, args...)
}

// Warnf logs a message at WARN level.
func (l *Logger) Warnf(typ string, format string, args ...any) {
	l.Log(WARN, typ, format, args...)
}

// Error logs a message at ERROR level.
func (l *Logger) Error(typ string, format string, args ...any) {
	l.Log(ERROR, typ, format, args...)
}

// Errorf logs a message at ERROR level.
func (l *Logger) Errorf(typ string, format string, args ...any) {
	l.Log(ERROR, typ, format, args...)
}

// WithType returns a BoundLogger associated with the specified module type.
func (l *Logger) WithType(typ string) *BoundLogger {
	return &BoundLogger{
		logger: l,
		typ:    typ,
	}
}

// WithModule is an alias for WithType.
func (l *Logger) WithModule(module string) *BoundLogger {
	return l.WithType(module)
}

// BoundLogger binds a logger instance to a fixed module type name.
type BoundLogger struct {
	logger *Logger
	typ    string
}

// Type returns the bound module type name.
func (b *BoundLogger) Type() string {
	return b.typ
}

// Logger returns the underlying Logger instance.
func (b *BoundLogger) Logger() *Logger {
	return b.logger
}

// Debug logs a message at DEBUG level using the bound module type.
func (b *BoundLogger) Debug(format string, args ...any) {
	b.logger.Log(DEBUG, b.typ, format, args...)
}

// Debugf logs a message at DEBUG level using the bound module type.
func (b *BoundLogger) Debugf(format string, args ...any) {
	b.logger.Log(DEBUG, b.typ, format, args...)
}

// Info logs a message at INFO level using the bound module type.
func (b *BoundLogger) Info(format string, args ...any) {
	b.logger.Log(INFO, b.typ, format, args...)
}

// Infof logs a message at INFO level using the bound module type.
func (b *BoundLogger) Infof(format string, args ...any) {
	b.logger.Log(INFO, b.typ, format, args...)
}

// Warn logs a message at WARN level using the bound module type.
func (b *BoundLogger) Warn(format string, args ...any) {
	b.logger.Log(WARN, b.typ, format, args...)
}

// Warnf logs a message at WARN level using the bound module type.
func (b *BoundLogger) Warnf(format string, args ...any) {
	b.logger.Log(WARN, b.typ, format, args...)
}

// Error logs a message at ERROR level using the bound module type.
func (b *BoundLogger) Error(format string, args ...any) {
	b.logger.Log(ERROR, b.typ, format, args...)
}

// Errorf logs a message at ERROR level using the bound module type.
func (b *BoundLogger) Errorf(format string, args ...any) {
	b.logger.Log(ERROR, b.typ, format, args...)
}

// Log writes a log entry with the bound module type.
func (b *BoundLogger) Log(level Level, format string, args ...any) {
	b.logger.Log(level, b.typ, format, args...)
}

// Logf writes a formatted log entry with the bound module type.
func (b *BoundLogger) Logf(level Level, format string, args ...any) {
	b.logger.Log(level, b.typ, format, args...)
}

// SetOutput updates the output destination for the default logger.
func SetOutput(w io.Writer) {
	defaultLogger.SetOutput(w)
}

// GetOutput returns the current output destination of the default logger.
func GetOutput() io.Writer {
	return defaultLogger.GetOutput()
}

// SetLevel updates the minimum logging level for the default logger.
func SetLevel(level Level) {
	defaultLogger.SetLevel(level)
}

// GetLevel returns the current minimum logging level of the default logger.
func GetLevel() Level {
	return defaultLogger.GetLevel()
}

// Log writes a log entry using the default logger.
func Log(level Level, typ string, format string, args ...any) {
	defaultLogger.Log(level, typ, format, args...)
}

// Logf writes a formatted log entry using the default logger.
func Logf(level Level, typ string, format string, args ...any) {
	defaultLogger.Log(level, typ, format, args...)
}

// Debug logs a message at LevelDebug using the default logger.
func Debug(typ string, format string, args ...any) {
	defaultLogger.Log(DEBUG, typ, format, args...)
}

// Debugf logs a message at LevelDebug using the default logger.
func Debugf(typ string, format string, args ...any) {
	defaultLogger.Log(DEBUG, typ, format, args...)
}

// Info logs a message at LevelInfo using the default logger.
func Info(typ string, format string, args ...any) {
	defaultLogger.Log(INFO, typ, format, args...)
}

// Infof logs a message at LevelInfo using the default logger.
func Infof(typ string, format string, args ...any) {
	defaultLogger.Log(INFO, typ, format, args...)
}

// Warn logs a message at LevelWarn using the default logger.
func Warn(typ string, format string, args ...any) {
	defaultLogger.Log(WARN, typ, format, args...)
}

// Warnf logs a message at LevelWarn using the default logger.
func Warnf(typ string, format string, args ...any) {
	defaultLogger.Log(WARN, typ, format, args...)
}

// Error logs a message at LevelError using the default logger.
func Error(typ string, format string, args ...any) {
	defaultLogger.Log(ERROR, typ, format, args...)
}

// Errorf logs a message at LevelError using the default logger.
func Errorf(typ string, format string, args ...any) {
	defaultLogger.Log(ERROR, typ, format, args...)
}

// WithType returns a BoundLogger using the default logger.
func WithType(typ string) *BoundLogger {
	return defaultLogger.WithType(typ)
}

// WithModule returns a BoundLogger using the default logger.
func WithModule(module string) *BoundLogger {
	return defaultLogger.WithModule(module)
}
