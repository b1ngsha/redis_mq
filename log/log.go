package log

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type Logger interface {
	Error(v ...interface{})
	Warn(v ...interface{})
	Info(v ...interface{})
	Debug(v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

var (
	defaultLogger Logger

	Levels = map[string]zapcore.Level{
		"":      zapcore.DebugLevel,
		"debug": zapcore.DebugLevel,
		"info":  zapcore.InfoLevel,
		"warn":  zapcore.WarnLevel,
		"error": zapcore.ErrorLevel,
		"fatal": zapcore.FatalLevel,
	}
)

func init() {
	defaultLogger = newSugarLogger(NewOptions())
}

type Options struct {
	LogName    string
	LogLevel   string
	FileName   string
	MaxAge     int  // day as unit
	MaxSize    int  // M as unit
	MaxBackups int  // backup files num
	Compress   bool // if compress
}

type Option func(*Options)

func NewOptions(opts ...Option) Options {
	options := Options{
		LogName:    "app",
		LogLevel:   "info",
		FileName:   "app.log",
		MaxAge:     10,
		MaxSize:    100,
		MaxBackups: 3,
		Compress:   true,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

func WithLogLevel(logLevel string) Option {
	return func(options *Options) { options.LogLevel = logLevel }
}

func WithFileName(fileName string) Option {
	return func(options *Options) { options.FileName = fileName }
}

type zapLoggerWrapper struct {
	*zap.SugaredLogger
	options Options
}

func (wrapper *zapLoggerWrapper) getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func (wrapper *zapLoggerWrapper) getLogWriter() zapcore.WriteSyncer {
	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   wrapper.options.FileName,
		MaxAge:     wrapper.options.MaxAge,
		MaxSize:    wrapper.options.MaxSize,
		MaxBackups: wrapper.options.MaxBackups,
		Compress:   wrapper.options.Compress,
	})
}

func newSugarLogger(options Options) *zapLoggerWrapper {
	wrapper := &zapLoggerWrapper{options: options}
	encoder := wrapper.getEncoder()
	writeSyncer := wrapper.getLogWriter()
	core := zapcore.NewCore(encoder, writeSyncer, Levels[options.LogLevel])
	wrapper.SugaredLogger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
	return wrapper
}

func Debugf(format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}

func DebugContext(ctx context.Context, args ...interface{}) {
	defaultLogger.Debug(args...)
}

func InfoContext(ctx context.Context, args ...interface{}) {
	defaultLogger.Info(args...)
}

func WarnContext(ctx context.Context, args ...interface{}) {
	defaultLogger.Warn(args...)
}

func ErrorContext(ctx context.Context, args ...interface{}) {
	defaultLogger.Error(args...)
}

func DebugContextf(ctx context.Context, format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

func InfoContextf(ctx context.Context, format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

func WarnContextf(ctx context.Context, format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

func ErrorContextf(ctx context.Context, format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	Errorf(format, args...)
}
