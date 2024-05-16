package db

import (
	"context"
	"errors"
	"github.com/xybingbing/pkg/log"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"sync"
	"time"
)

const (
	TypeSQLite     = "sqllite"
	TypeMySQL      = "mysql"
	TypePostgreSQL = "postgres"
)

/*
CREATE TABLE `example` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='表备注';
*/

type Config struct {
	Logger           *log.Logger
	Type             string `default:"mysql"`
	DSN              string `default:"-"`
	MaxIdleConn      int    `default:"10"`    // 最大空闲连接数，默认10
	MaxOpenConn      int    `default:"100"`   // 最大活动连接数，默认100
	ConnMaxLifetime  int    `default:"300"`   // 连接的最大存活时间，默认300s
	ConnMaxIdleTime  int    `default:"300"`   // 连接的最大空闲时间，默认300s
	SlowLogThreshold int    `default:"1000"`  // 慢日志阈值，默认1000ms
	EnableDebug      bool   `default:"false"` // 是否开启调试
	EnableMetric     bool   `default:"false"` // 是否开启监控
	EnableTrace      bool   `default:"true"`  // 是否开启链路追踪，默认开启
	dbName           string
	interceptors     []Interceptor
}

var instances = sync.Map{}

type Wrapper struct {
	db *gorm.DB
}

func (wrap *Wrapper) GetDB() *gorm.DB {
	return wrap.db
}

func GetSession(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.Session(&gorm.Session{NewDB: true, Context: ctx})
}

func NewWrapper(config *Config, obj ...string) (*Wrapper, error) {
	name := "default"
	if len(obj) > 0 {
		name = obj[0]
	}
	if value, ok := instances.Load(name); ok && value != nil {
		return value.(*Wrapper), nil
	}
	//创建链接
	gormDB, err := newDB(config)
	if err != nil {
		return nil, err
	}
	wrapper := &Wrapper{
		db: gormDB,
	}
	instances.Store(name, wrapper)
	return wrapper, nil
}

func newDB(config *Config) (*gorm.DB, error) {
	var (
		gormDB *gorm.DB
		err    error
	)

	logger := NewZapLog(config.Logger.Logger)

	switch config.Type {
	case TypeSQLite:
		gormDB, err = gorm.Open(sqlite.Open(config.DSN), &gorm.Config{
			Logger: logger,
		})
		if err != nil {
			return nil, err
		}
	case TypeMySQL:
		gormDB, err = gorm.Open(mysql.Open(config.DSN), &gorm.Config{
			Logger: logger,
		})
		if err != nil {
			return nil, err
		}
	case TypePostgreSQL:
		gormDB, err = gorm.Open(postgres.Open(config.DSN), &gorm.Config{
			Logger: logger,
		})
		if err != nil {
			return nil, err
		}
	}

	if gormDB == nil {
		return nil, errors.New("gormDB is nil")
	}

	db, err := gormDB.DB()
	if err != nil {
		return nil, err
	}

	// 设置默认连接配置
	db.SetMaxIdleConns(config.MaxIdleConn)
	db.SetMaxOpenConns(config.MaxOpenConn)
	if config.ConnMaxLifetime != 0 {
		db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetime) * time.Second)
	}
	if config.ConnMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(time.Duration(config.ConnMaxIdleTime) * time.Second)
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	err = db.QueryRow("SELECT DATABASE()").Scan(&config.dbName)
	if err != nil {
		return nil, err
	}
	//拦截器
	config.interceptors = make([]Interceptor, 0)
	if config.EnableDebug {
		config.interceptors = append(config.interceptors, debugInterceptor)
	}
	if config.EnableTrace {
		config.interceptors = append(config.interceptors, traceInterceptor)
	}
	if config.EnableMetric {
		config.interceptors = append(config.interceptors, metricInterceptor)
	}
	replace := func(processor Processor, callbackName string, interceptors ...Interceptor) error {
		handler := processor.Get(callbackName)
		for _, interceptor := range config.interceptors {
			handler = interceptor(callbackName, config)(handler)
		}
		return processor.Replace(callbackName, handler)
	}
	if err = replace(gormDB.Callback().Create(), "gorm:create", config.interceptors...); err != nil {
		return nil, err
	}
	if err = replace(gormDB.Callback().Update(), "gorm:update", config.interceptors...); err != nil {
		return nil, err
	}
	if err = replace(gormDB.Callback().Delete(), "gorm:delete", config.interceptors...); err != nil {
		return nil, err
	}
	if err = replace(gormDB.Callback().Query(), "gorm:query", config.interceptors...); err != nil {
		return nil, err
	}
	if err = replace(gormDB.Callback().Row(), "gorm:row", config.interceptors...); err != nil {
		return nil, err
	}
	if err = replace(gormDB.Callback().Raw(), "gorm:raw", config.interceptors...); err != nil {
		return nil, err
	}
	return gormDB, nil
}

//================================================================================

type Page struct {
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
	OrderBy  string `json:"order_by"`
}

func PageOption(session *gorm.DB, pg *Page) *gorm.DB {
	if pg == nil {
		return session
	}
	if pg.Page == 0 {
		pg.Page = 1
	}
	if pg.PageSize == 0 {
		pg.PageSize = 100
	}
	offset := (pg.Page - 1) * pg.PageSize
	session = session.Offset(int(offset)).Limit(int(pg.PageSize))
	if pg.OrderBy != "" {
		session = session.Order(pg.OrderBy)
	}
	return session
}

type Filters map[string]interface{}

func (list Filters) Execute(db *gorm.DB) *gorm.DB {
	for key, val := range list {
		db = db.Where(key, val)
	}
	return db
}
