package db

import (
	"context"
	"encoding/json"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"log"
	"net/http"
	"time"
)

func init() {
	http.HandleFunc("/debug/db/stats", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(stats())
	})
	go monitor()
}

func monitor() {
	listMonitor := []string{}
	for {
		instances.Range(func(key, val interface{}) bool {
			name := key.(string)
			for _, monitorName := range listMonitor {
				if monitorName == name {
					return true
				}
			}
			log.Print("开启监听【Gorm】自带指标：", name)
			listMonitor = append(listMonitor, name)
			meter := otel.Meter("grom")
			wrap := val.(*Wrapper)
			sqlDB, err := wrap.db.DB()
			if err != nil {
				log.Print("monitor db error", err)
				return false
			}
			// Gauge指标
			_, _ = meter.Float64ObservableGauge("grom_max_open_connections", metric.WithDescription("最大打开连接数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().MaxOpenConnections))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_open_connections", metric.WithDescription("已打开的连接数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().OpenConnections))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_in_use", metric.WithDescription("正在使用的连接数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().InUse))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_idle", metric.WithDescription("空闲连接数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().Idle))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_max_idle_closed", metric.WithDescription("超过最大 idle 数所关闭的连接总数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().MaxIdleClosed))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_max_idle_time_closed", metric.WithDescription("超过追到 idle 时间所关闭的连接总数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().MaxIdleTimeClosed))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_max_lifetime_closed", metric.WithDescription("超过最大生命周期所关闭的连接总数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().MaxLifetimeClosed))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_wait_count", metric.WithDescription("等待连接数"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().WaitCount))
				return nil
			}))
			_, _ = meter.Float64ObservableGauge("grom_wait_duration", metric.WithDescription("等待总耗时"), metric.WithFloat64Callback(func(ctx context.Context, observer metric.Float64Observer) error {
				observer.Observe(float64(sqlDB.Stats().WaitDuration.Milliseconds() / 1000))
				return nil
			}))
			return true
		})
		time.Sleep(30 * time.Second)
	}
}

// stats
func stats() (stats map[string]interface{}) {
	stats = make(map[string]interface{})
	instances.Range(func(key, val interface{}) bool {
		name := key.(string)
		wrap := val.(*Wrapper)
		sqlDB, err := wrap.db.DB()
		if err != nil {
			log.Panic("stats db error", err)
			return false
		}
		stats[name] = sqlDB.Stats()
		return true
	})
	return
}
