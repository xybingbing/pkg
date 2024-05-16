package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm/logger"
)

type Logger struct {
	ZapLogger                 *zap.Logger
	SlowThreshold             time.Duration
	Colorful                  bool
	IgnoreRecordNotFoundError bool
	ParameterizedQueries      bool
	LogLevel                  logger.LogLevel
}

func NewZapLog(zapLogger *zap.Logger) logger.Interface {
	return &Logger{
		ZapLogger:                 zapLogger,
		LogLevel:                  logger.Warn,
		SlowThreshold:             1000 * time.Millisecond,
		Colorful:                  false,
		IgnoreRecordNotFoundError: false,
		ParameterizedQueries:      false,
	}
}

func (l *Logger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

func (l Logger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Info {
		l.zapLogger().Sugar().Infof(msg, data...)
	}
}

func (l Logger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Warn {
		l.zapLogger().Sugar().Warnf(msg, data...)
	}
}

func (l Logger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Error {
		l.zapLogger().Sugar().Errorf(msg, data...)
	}
}

func (l Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}
	elapsed := time.Since(begin)
	elapsedStr := fmt.Sprintf("%.3fms", float64(elapsed.Nanoseconds())/1e6)
	zapLogger := l.zapLogger()
	if err != nil && (!errors.Is(err, logger.ErrRecordNotFound) || !l.IgnoreRecordNotFoundError) {
		sql, rows := fc()
		if rows == -1 {
			zapLogger.Error("trace", zap.Error(err), zap.String("elapsed", elapsedStr), zap.Int64("rows", rows), zap.String("sql", sql))
		} else {
			zapLogger.Error("trace", zap.Error(err), zap.String("elapsed", elapsedStr), zap.Int64("rows", rows), zap.String("sql", sql))
		}
	}
}

var (
	gormPackage = filepath.Join("gorm.io", "gorm")
)

func (l Logger) zapLogger() *zap.Logger {
	zapLogger := l.ZapLogger
	for i := 2; i < 15; i++ {
		_, file, _, ok := runtime.Caller(i)
		switch {
		case !ok:
		case strings.HasSuffix(file, "_test.go"):
		case strings.Contains(file, gormPackage):
		default:
			return zapLogger.WithOptions(zap.AddCallerSkip(i - 1))
		}
	}
	return zapLogger
}
