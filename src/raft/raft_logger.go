package raft

import (
	"fmt"
	"go.uber.org/zap"
	"os"
	"strconv"
)

func GetLogger() *zap.SugaredLogger {
	config := zap.NewDevelopmentConfig()
	//set this to remove logging
	if os.Getenv("log_process") != "true" {
		config.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
	}
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.StacktraceKey = "" // to hide stacktrace info
	encoderConfig.CallerKey = ""     // to hide callee
	config.EncoderConfig = encoderConfig

	zapLog, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}

	return zapLog.Sugar()
}

func (rf *Raft) getLoggerPrefix() string {
	return "[Peer : " + strconv.Itoa(rf.GetSelfPeerIndex()) + "] [Term : " + strconv.Itoa(int(rf.stable.GetTermManager().GetTerm())) + "] [State : " + rf.stable.GetTermManager().GetCurrentState().String() + "] "
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
