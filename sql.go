package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"maps"
	"net/url"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
	"github.com/webcore-go/webcore/app/config"
	"github.com/webcore-go/webcore/app/loader"
	"github.com/webcore-go/webcore/app/logger"
)

type SQLDB interface {
	loader.IDatabase
}

// SQLDatabase represents shared database connection using Bun ORM
type SQLDatabase struct {
	Context context.Context
	Config  config.DatabaseConfig
	Driver  driver.Connector
	Dialect schema.Dialect
	DB      *bun.DB
}

func (d *SQLDatabase) SetBunDB(driver driver.Connector, dialect schema.Dialect) {
	d.Driver = driver
	d.Dialect = dialect
}

// Install library
func (d *SQLDatabase) Install(args ...any) error {
	if len(args) < 2 {
		return fmt.Errorf("Install requires 2 arguments: context and config")
	}

	ctx, ok := args[0].(context.Context)
	if !ok {
		return fmt.Errorf("first argument must be context.Context")
	}
	d.Context = ctx

	cfg, ok := args[1].(config.DatabaseConfig)
	if !ok {
		return fmt.Errorf("second argument must be config.DatabaseConfig")
	}
	d.Config = cfg
	return nil
}

// Connect establishes a database connection
func (d *SQLDatabase) Connect() error {
	// Create Bun DB instance
	sqldb := sql.OpenDB(d.Driver)
	d.DB = bun.NewDB(sqldb, d.Dialect)

	return nil
}

// Close closes the database connection
func (d *SQLDatabase) Disconnect() error {
	if d.DB == nil {
		return fmt.Errorf("database not connected")
	}
	return d.DB.Close()
}

// Connect establishes a database connection
func (d *SQLDatabase) Uninstall() error {
	return nil
}

// Ping checks if database connection is alive
func (d *SQLDatabase) Ping(ctx context.Context) error {
	if d.DB == nil {
		return fmt.Errorf("database not connected")
	}
	return d.DB.PingContext(ctx)
}

// GetConnection returns the underlying database connection
func (d *SQLDatabase) GetConnection() any {
	return d.DB
}

// GetDriver returns database driver name
func (d *SQLDatabase) GetDriver() string {
	return d.Config.Driver
}

// GetName returns database name
func (d *SQLDatabase) GetName() string {
	return d.Config.Name
}

// Count counts records in a table with optional filtering
func (d *SQLDatabase) Count(ctx context.Context, table string, filter []loader.DbExpression) (int64, error) {
	var count int64
	query := d.DB.NewSelect().ColumnExpr("COUNT(*)").Table(table)

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	err := query.Scan(ctx, &count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// Find retrieves records from a table with optional filtering, sorting, and pagination
func (d *SQLDatabase) Find(ctx context.Context, results any, table string, column []string, filter []loader.DbExpression, sort map[string]int, limit int64, skip int64) error {
	query := d.DB.NewSelect().Table(table)

	if len(column) > 0 {
		query.Column(column...)
	}

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	for field, direction := range sort {
		if direction > 0 {
			query.Order(fmt.Sprintf("%s ASC", field))
		} else {
			query.Order(fmt.Sprintf("%s DESC", field))
		}
	}

	if limit > 0 {
		query.Limit(int(limit))
	}

	if skip > 0 {
		query.Offset(int(skip))
	}

	err := query.Scan(ctx, results)
	if err != nil {
		return err
	}

	return nil
}

// FindOne retrieves a single record from a table
func (d *SQLDatabase) FindOne(ctx context.Context, result any, table string, column []string, filter []loader.DbExpression, sort map[string]int) error {
	query := d.DB.NewSelect().Table(table)

	if len(column) > 0 {
		query.Column(column...)
	}

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	for field, direction := range sort {
		if direction > 0 {
			query.Order(fmt.Sprintf("%s ASC", field))
		} else {
			query.Order(fmt.Sprintf("%s DESC", field))
		}
	}

	return query.Limit(1).Scan(ctx, result)
}

// InsertOne inserts a single record into a table
func (d *SQLDatabase) InsertOne(ctx context.Context, _ string, data any) (any, error) {
	_, err := d.DB.NewInsert().Model(data).Exec(ctx)
	if err != nil {
		return nil, err
	}

	// Return inserted data with ID
	return data, nil
}

// Update updates records in a table with optional filtering
func (d *SQLDatabase) Update(ctx context.Context, _ string, filter []loader.DbExpression, data any) (int64, error) {
	query := d.DB.NewUpdate().Model(data)

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	result, err := query.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// UpdateOne updates a single record in a table
func (d *SQLDatabase) UpdateOne(ctx context.Context, _ string, filter []loader.DbExpression, data any) (int64, error) {
	query := d.DB.NewUpdate().Model(data)

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	result, err := query.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// Delete deletes records from a table with optional filtering
func (d *SQLDatabase) Delete(ctx context.Context, table string, filter []loader.DbExpression) (int64, error) {
	query := d.DB.NewDelete().Table(table)

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	result, err := query.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// DeleteOne deletes a single record from a table
func (d *SQLDatabase) DeleteOne(ctx context.Context, table string, filter []loader.DbExpression) (int64, error) {
	query := d.DB.NewDelete().Table(table)

	buildWhereClause(d.Config.Driver, query.QueryBuilder(), filter, "")

	result, err := query.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// BuildDSN builds a database connection string from configuration
func BuildDSN(config config.DatabaseConfig) string {
	var dsn string
	switch config.Driver {
	case "postgres":
		// dsn = fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s",
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			config.User, config.Password, config.Host, config.Port, config.Name, config.SSLMode)
		if len(config.Attributes) > 0 {
			queryParams := url.Values{}
			for key, value := range config.Attributes {
				queryParams.Add(key, value)
			}
			if len(queryParams) > 0 {
				dsn += "?" + queryParams.Encode()
			}
		}
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			config.User, config.Password, config.Host, config.Port, config.Name)

		attributes := make(map[string]string)
		maps.Copy(attributes, config.Attributes)

		if _, exists := attributes["charset"]; !exists {
			attributes["charset"] = "utf8mb4"
		}
		if _, exists := attributes["parseTime"]; !exists {
			attributes["parseTime"] = "True"
		}
		if _, exists := attributes["loc"]; !exists {
			attributes["loc"] = "Local"
		}

		queryParams := []string{}
		for key, value := range attributes {
			queryParams = append(queryParams, fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
		}
		if len(queryParams) > 0 {
			dsn += "?" + strings.Join(queryParams, "&")
		}
	case "sqlite":
		dsn = config.Name
		if len(config.Attributes) > 0 {
			queryParams := []string{}
			for key, value := range config.Attributes {
				queryParams = append(queryParams, fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
			}
			if len(queryParams) > 0 {
				dsn += "?" + strings.Join(queryParams, "&")
			}
		}
	}

	logger.Debug("DSN", "dsn", dsn)
	return dsn
}

// buildWhereClause converts MongoDB-style filter to SQL WHERE clause
func buildWhereClause(driver string, builder bun.QueryBuilder, filter []loader.DbExpression, andOr string) bun.QueryBuilder {
	if len(filter) == 0 {
		return builder
	}

	if andOr == "" {
		andOr = "AND"
	}

	for _, value := range filter {
		if value.Op != "" {
			switch value.Op {
			case "IN":
			case "NOT IN":
			case "ANY =":
			case "ANY !=":
			case "ANY >":
			case "ANY >=":
			case "ANY <":
			case "ANY <=":
				if andOr == "OR" {
					builder.WhereOr(fmt.Sprintf("%s %s (?)", value.Expr, value.Op), value.Args...)
				} else {
					builder.Where(fmt.Sprintf("%s %s (?)", value.Expr, value.Op), value.Args...)
				}
			case "ANY":
				if andOr == "OR" {
					builder.WhereOr(fmt.Sprintf("%s = ANY (?)", value.Expr), value.Args...)
				} else {
					builder.Where(fmt.Sprintf("%s = ANY (?)", value.Expr), value.Args...)
				}
			case "NOT ANY":
				if andOr == "OR" {
					builder.WhereOr(fmt.Sprintf("%s != ANY (?)", value.Expr), value.Args...)
				} else {
					builder.Where(fmt.Sprintf("%s != ANY (?)", value.Expr), value.Args...)
				}
			case "ANY LIKE":
			case "ANY ILIKE":
				op := value.Op
				if driver != "postgres" {
					op = "LIKE"
				}
				if andOr == "OR" {
					builder.WhereOr(fmt.Sprintf("%s %s ANY (?)", value.Expr, op), value.Args...)
				} else {
					builder.Where(fmt.Sprintf("%s %s ANY (?)", value.Expr, op), value.Args...)
				}
			case "LIKE":
			case "ILIKE":
				op := value.Op
				if driver != "postgres" {
					op = "LIKE"
				}
				if andOr == "OR" {
					builder.WhereOr(fmt.Sprintf("%s %s ?", value.Expr, op), value.Args...)
				} else {
					builder.Where(fmt.Sprintf("%s %s ?", value.Expr, op), value.Args...)
				}
			case "GROUP_OR":
				ln := len(value.Args)
				if ln > 0 {
					conditions := make([]loader.DbExpression, ln)
					for i, arg := range value.Args {
						if cond, ok := arg.(loader.DbExpression); ok {
							conditions[i] = cond
						}
					}
					builder.WhereGroup(" AND ", func(q bun.QueryBuilder) bun.QueryBuilder {
						return buildWhereClause(driver, q, conditions, "OR")
					})
				}
			case "GROUP_AND":
				ln := len(value.Args)
				if ln > 0 {
					conditions := make([]loader.DbExpression, ln)
					for i, arg := range value.Args {
						if cond, ok := arg.(loader.DbExpression); ok {
							conditions[i] = cond
						}
					}
					builder.WhereGroup(" AND ", func(q bun.QueryBuilder) bun.QueryBuilder {
						return buildWhereClause(driver, q, conditions, "AND")
					})
				}
			default:
				if len(value.Args) > 0 {
					switch value.Args[0] {
					case nil:
						builder.Where(fmt.Sprintf("%s IS NULL", value.Expr))
					case true:
						builder.Where(fmt.Sprintf("%s", value.Expr))
					case false:
						builder.Where(fmt.Sprintf("NOT %s", value.Expr))
					default:
						builder.Where(fmt.Sprintf("%s %s ?", value.Expr, value.Op), value.Args[0])
					}
				}
			}
		} else {
			isExp := strings.Contains(value.Expr, "?")
			if !isExp {
				if len(value.Args) > 0 {

					switch value.Args[0] {
					case nil:
						builder.Where(fmt.Sprintf("%s IS NULL", value.Expr))
					case true:
						builder.Where(fmt.Sprintf("%s", value.Expr))
					case false:
						builder.Where(fmt.Sprintf("NOT %s", value.Expr))
					default:
						if value.Op == "" {
							builder.Where(fmt.Sprintf("%s = ?", value.Expr), value.Args...)
						} else {
							builder.Where(fmt.Sprintf("%s %s ?", value.Expr, value.Op), value.Args...)
						}
					}
				}
			} else {
				builder.Where(value.Expr, value.Args...)
			}
		}
	}

	return builder
}
