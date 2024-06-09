package utils

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

type Logger struct {
	logger    *zap.SugaredLogger // logger to log stuff
	getPrefix func() string      // prefix
}

func (lg *Logger) LogDebug(args ...interface{}) {
	lg.logger.Debug(lg.getPrefix(), fmt.Sprintf("%+v", args))
}

func (lg *Logger) LogInfo(args ...interface{}) {
	lg.logger.Info(lg.getPrefix(), fmt.Sprintf("%+v", args))
}

func (lg *Logger) LogWarn(args ...interface{}) {
	lg.logger.Warn(lg.getPrefix(), fmt.Sprintf("%+v", args))
}

func (lg *Logger) LogError(args ...interface{}) {
	lg.logger.Error(lg.getPrefix(), fmt.Sprintf("%+v", args))
}

func (lg *Logger) LogPanic(args ...interface{}) {
	lg.logger.Panic(lg.getPrefix(), fmt.Sprintf("%+v", args))
}

func (lg *Logger) LogFatal(args ...interface{}) {
	lg.logger.Fatal(lg.getPrefix(), fmt.Sprintf("%+v", args))
}

func GetEnvLogLevel(envVar string) zapcore.Level {
	level := os.Getenv(envVar)

	switch level {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.FatalLevel
	}
}

func GetLogger(logEnvVar string, prefixCreator func() string) Logger {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.StacktraceKey = "" // to hide stacktrace info
	encoderConfig.CallerKey = ""     // to hide callee
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(GetEnvLogLevel(logEnvVar))
	config.EncoderConfig = encoderConfig

	zapLog, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}

	paddedPrefixCreator := func() string {
		return fmt.Sprintf("%-50s", prefixCreator())
	}

	return Logger{logger: zapLog.Sugar(), getPrefix: paddedPrefixCreator}
}
