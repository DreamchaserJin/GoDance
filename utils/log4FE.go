package utils

import (
	"fmt"
	"os"
	"path"
	"runtime"

	"github.com/apsdehal/go-logger"
)

type Log4FE struct {
	service_name  string
	service_env   string
	file_handle   *os.File
	logger_handle *logger.Logger
}

func New(service string) (log4FE *Log4FE, err error) {
	// TODO: 从配置文件里面读取：日志模式、日志路径、日志缓存
	filename := fmt.Sprintf("/var/log/FalconEngine/logs/%s.log", service)

	// 初始化Log4FE
	out, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		filename = fmt.Sprintf("%s.log", service)
		out, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
	}
	logtmp, _ := logger.New(filename, out)

	env_deploy := os.Getenv("ENV_DEPLOY")
	if env_deploy == "" {
		env_deploy = "TESTING"
	}

	log4FE = &Log4FE{
		service_name:  service,
		service_env:   env_deploy,
		file_handle:   out,
		logger_handle: logtmp,
	}

	return log4FE, nil
}

func (l *Log4FE) Close() (err error) {
	return l.file_handle.Close()
}

func (l *Log4FE) log(level string, format string, args ...interface{}) (err error) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(msg)

	_, filepath, filenum, _ := runtime.Caller(2)
	filename := path.Base(filepath)
	logmsg := fmt.Sprintf("%s %s %s %s %d - %s", l.service_name, l.service_env, level, filename, filenum, msg)
	l.logger_handle.Log(logger.DebugLevel, logmsg)

	return nil
}

func (l *Log4FE) Fatal(format string, args ...interface{}) (err error) {
	return l.log("FATAL", format, args...)
}

func (l *Log4FE) Error(format string, args ...interface{}) (err error) {
	return l.log("ERROR", format, args...)
}

func (l *Log4FE) Warn(format string, args ...interface{}) (err error) {
	return l.log("WARN", format, args...)
}

func (l *Log4FE) Info(format string, args ...interface{}) (err error) {
	return l.log("INFO", format, args...)
}

func (l *Log4FE) Debug(format string, args ...interface{}) (err error) {
	return l.log("DEBUG", format, args...)
}

func (l *Log4FE) Trace(format string, args ...interface{}) (err error) {
	return l.log("TRACE", format, args...)
}
