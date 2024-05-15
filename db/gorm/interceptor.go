package gorm

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"log"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"gorm.io/gorm"
	"gorm.io/hints"
)

// Handler ...
type Handler func(*gorm.DB)

// Processor ...
type Processor interface {
	Get(name string) func(*gorm.DB)
	Replace(name string, handler func(*gorm.DB)) error
}

// Interceptor ...
type Interceptor func(string, *Config) func(next Handler) Handler

// debug拦截器
func debugInterceptor(callbackName string, config *Config) func(Handler) Handler {
	return func(next Handler) Handler {
		return func(db *gorm.DB) {
			beg := time.Now()
			next(db)
			cost := time.Since(beg)
			if db.Error != nil {
				log.Println("[db.response]", cost, logSQL(db), db.Error.Error())
			} else {
				log.Println("[db.response]", cost, logSQL(db))
			}
		}
	}
}

// metric拦截器
func metricInterceptor(callbackName string, config *Config) func(Handler) Handler {
	return func(next Handler) Handler {
		return func(db *gorm.DB) {
			beg := time.Now()
			next(db)
			cost := time.Since(beg)

			ctx := context.Background()
			meter := otel.Meter(callbackName)
			histogram, _ := meter.Float64Histogram("gorm_query_cost", metric.WithDescription("查询数据耗时"))
			if histogram != nil {
				histogram.Record(ctx, cost.Seconds(), metric.WithAttributes(attribute.String("dbName", config.dbName), attribute.String("table", db.Statement.Table)))
			}

		}
	}
}

func traceInterceptor(callbackName string, config *Config) func(Handler) Handler {
	return func(next Handler) Handler {
		return func(db *gorm.DB) {
			if db.Statement.Context != nil {
				operation := "gorm:"
				if len(db.Statement.BuildClauses) > 0 {
					operation += strings.ToLower(db.Statement.BuildClauses[0])
				}
				_, span := otel.Tracer(callbackName).Start(db.Statement.Context, operation, nil)
				defer span.End()
				comment := fmt.Sprintf("traceId=%s", span.SpanContext().TraceID().String())
				if db.Statement.SQL.Len() > 0 {
					sql := db.Statement.SQL.String()
					db.Statement.SQL.Reset()
					db.Statement.SQL.WriteString("/* ")
					db.Statement.SQL.WriteString(comment)
					db.Statement.SQL.WriteString(" */ ")
					db.Statement.SQL.WriteString(sql)
				} else {
					hints.CommentBefore("SELECT", comment).ModifyStatement(db.Statement)
					hints.CommentBefore("INSERT", comment).ModifyStatement(db.Statement)
					hints.CommentBefore("UPDATE", comment).ModifyStatement(db.Statement)
					hints.CommentBefore("DELETE", comment).ModifyStatement(db.Statement)
				}
				next(db)
				span.SetAttributes(
					semconv.DBSystemKey.String(db.Dialector.Name()),
					semconv.DBStatementKey.String(logSQL(db)),
					semconv.DBOperationKey.String(operation),
					semconv.DBSQLTableKey.String(db.Statement.Table),
					attribute.Int64("db.rows_affected", db.RowsAffected),
				)
				var err = db.Error
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					span.RecordError(db.Error)
					span.SetStatus(codes.Error, db.Error.Error())
					return
				}
				span.SetStatus(codes.Ok, "OK")
				return
			}
			next(db)
		}
	}
}

func logSQL(db *gorm.DB) string {
	return db.Explain(db.Statement.SQL.String(), db.Statement.Vars...)
}
