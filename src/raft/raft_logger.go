package raft

import (
	"go.uber.org/zap"
	"strconv"
)

func GetLogger() *zap.SugaredLogger {
	config := zap.NewDevelopmentConfig()
	enccoderConfig := zap.NewDevelopmentEncoderConfig()
	enccoderConfig.StacktraceKey = "" // to hide stacktrace info
	enccoderConfig.CallerKey = ""     // to hide callee
	config.EncoderConfig = enccoderConfig

	zapLog, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		panic(err)
	}

	return zapLog.Sugar()
}

func (rf *Raft) getLoggerPrefix() string {
	return "[Peer : " + strconv.Itoa(rf.GetSelfPeerIndex()) + "] [Term : " + strconv.Itoa(int(rf.GetTermManager().GetTerm())) + "] [State : " + rf.GetTermManager().GetCurrentState().String() + "] "
}

func (rf *Raft) LogInfo(args ...interface{}) {
	rf.logger.Info(rf.getLoggerPrefix(), args)
}

func (rf *Raft) LogDebug(args ...interface{}) {
	rf.logger.Debug(rf.getLoggerPrefix(), args)
}

func (rf *Raft) LogError(args ...interface{}) {
	rf.logger.Error(rf.getLoggerPrefix(), args)
}

func (rf *Raft) LogFatal(args ...interface{}) {
	rf.logger.Fatal(rf.getLoggerPrefix(), args)
}

func (rf *Raft) LogWarn(args ...interface{}) {
	rf.logger.Warn(rf.getLoggerPrefix(), args)
}
