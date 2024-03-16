package raft

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strconv"
)

func GetLogger() *zap.SugaredLogger {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.StacktraceKey = "" // to hide stacktrace info
	encoderConfig.CallerKey = ""     // to hide callee
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(getLogLevel())
	config.EncoderConfig = encoderConfig

	zapLog, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}

	return zapLog.Sugar()
}

func (rf *Raft) getLoggerPrefix() string {
	termManager := rf.stable.GetTermManager()
	return "[Peer : " + strconv.Itoa(rf.GetSelfPeerIndex()) + "] [Term : " + strconv.Itoa(int(termManager.GetTerm())) + "] [State : " + termManager.GetCurrentState().String() + "] "
}

func (rf *Raft) LogInfo(args ...interface{}) {
	rf.logger.Info(rf.getLoggerPrefix(), fmt.Sprintf("%+v", args))
}

func (rf *Raft) LogDebug(args ...interface{}) {
	rf.logger.Debug(rf.getLoggerPrefix(), fmt.Sprintf("%+v", args))
}

func (rf *Raft) LogError(args ...interface{}) {
	rf.logger.Error(rf.getLoggerPrefix(), fmt.Sprintf("%+v", args))
}

func (rf *Raft) LogFatal(args ...interface{}) {
	rf.logger.Fatal(rf.getLoggerPrefix(), fmt.Sprintf("%+v", args))
}

func (rf *Raft) LogWarn(args ...interface{}) {
	rf.logger.Warn(rf.getLoggerPrefix(), fmt.Sprintf("%+v", args))
}

func getLogLevel() zapcore.Level {
	level := os.Getenv("logging_level")

	switch level {
	case "warn":
		return zap.WarnLevel
	case "info":
		return zap.InfoLevel
	case "debug":
		return zap.DebugLevel
	default:
		return zap.FatalLevel
	}
}
