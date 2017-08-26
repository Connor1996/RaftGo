package raft

import (
	"log"
	"io"
	"io/ioutil"
)

type Logger struct {
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
}

// init logger
func (logger *Logger) InitLogger(traceHandle io.Writer, infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer) {
	logger.Trace = log.New(traceHandle,
		"TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)

	logger.Info = log.New(infoHandle,
		"INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

	logger.Warning = log.New(warningHandle,
		"WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)

	logger.Error = log.New(errorHandle,
		"ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}


func (logger *Logger) DisableLogger() {
	logger.Trace.SetOutput(ioutil.Discard)
	logger.Info.SetOutput(ioutil.Discard)
	logger.Warning.SetOutput(ioutil.Discard)
	logger.Error.SetOutput(ioutil.Discard)
}

func (logger *Logger) EnableLogger(traceHandle io.Writer, infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer) {
	logger.Trace.SetOutput(traceHandle)
	logger.Info.SetOutput(infoHandle)
	logger.Warning.SetOutput(warningHandle)
	logger.Error.SetOutput(errorHandle)
}
