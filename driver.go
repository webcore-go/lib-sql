package sql

import (
	"context"
	"database/sql/driver"
	"fmt"
)

var drivers = map[string]driver.Connector{}

type Driver struct {
	connector *Connector
}

var _ driver.DriverContext = (*Driver)(nil)

func NewDriver() Driver {
	return Driver{}
}

func (d Driver) OpenConnector(name string) (driver.Connector, error) {
	if driver, ok := drivers[name]; ok {
		return NewConnector(name, driver), nil
	}
	return nil, fmt.Errorf("Driver %s not found", name)
}

func (d Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.TODO())
}

type Connector struct {
	driver driver.Connector
}

func NewConnector(name string, driver driver.Connector) *Connector {
	if _, ok := drivers[name]; !ok {
		drivers[name] = driver
	}
	return &Connector{
		driver: driver,
	}
}

var _ driver.Connector = (*Connector)(nil)

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return newConn(ctx, c.driver)
}

func (c *Connector) Driver() driver.Driver {
	return Driver{connector: c}
}

// newConn creates a new database connection based on the driver type
// This is similar to pgdriver.NewConnector for PostgreSQL
func newConn(ctx context.Context, connector driver.Connector) (driver.Conn, error) {
	return connector.Connect(ctx)
}

// toNamedValues converts driver.Value slice to any slice
func ToNamedValues(args []driver.Value) []any {
	result := make([]any, len(args))
	for i, arg := range args {
		result[i] = arg
	}
	return result
}
