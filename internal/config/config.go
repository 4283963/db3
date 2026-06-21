package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration loaded from the yaml file and
// optionally overridden by environment variables.
type Config struct {
	Server ServerConfig `yaml:"server"`
	MySQL  MySQLConfig  `yaml:"mysql"`
	IDGen  IDGenConfig  `yaml:"idgen"`
}

// ServerConfig controls the Gin HTTP server.
type ServerConfig struct {
	Port int    `yaml:"port"`
	Mode string `yaml:"mode"`
}

// MySQLConfig controls the database connection pool. If DSN is empty it is
// assembled from Host/Port/User/Password/DBName/Charset.
type MySQLConfig struct {
	DSN                string `yaml:"dsn"`
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
	User               string `yaml:"user"`
	Password           string `yaml:"password"`
	DBName             string `yaml:"dbname"`
	Charset            string `yaml:"charset"`
	MaxOpenConns       int    `yaml:"max_open_conns"`
	MaxIdleConns       int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeSec int    `yaml:"conn_max_lifetime_sec"`
}

// BuildDSN returns the connection string used by the mysql driver. When a
// ready-made DSN is configured it is returned verbatim, otherwise the DSN is
// constructed from the individual fields.
func (m MySQLConfig) BuildDSN() string {
	if m.DSN != "" {
		return m.DSN
	}
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local&multiStatements=true",
		m.User, m.Password, m.Host, m.Port, m.DBName, m.Charset,
	)
}

// IDGenConfig configures the snowflake algorithm.
type IDGenConfig struct {
	Epoch              string `yaml:"epoch"`
	MaxClockBackwardMs int64  `yaml:"max_clock_backward_ms"`

	epochTime time.Time
}

// EpochTime returns the parsed custom epoch.
func (c IDGenConfig) EpochTime() time.Time { return c.epochTime }

// Load reads and parses the configuration file at path, applies built-in
// defaults, validates the result and finally applies environment overrides.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyEnvOverrides()

	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.Mode == "" {
		c.Server.Mode = "debug"
	}
	if c.MySQL.Host == "" {
		c.MySQL.Host = "127.0.0.1"
	}
	if c.MySQL.Port == 0 {
		c.MySQL.Port = 3306
	}
	if c.MySQL.DBName == "" {
		c.MySQL.DBName = "db3_idgen"
	}
	if c.MySQL.Charset == "" {
		c.MySQL.Charset = "utf8mb4"
	}
	if c.MySQL.MaxOpenConns == 0 {
		c.MySQL.MaxOpenConns = 50
	}
	if c.MySQL.MaxIdleConns == 0 {
		c.MySQL.MaxIdleConns = 10
	}
	if c.MySQL.ConnMaxLifetimeSec == 0 {
		c.MySQL.ConnMaxLifetimeSec = 300
	}
	if c.IDGen.Epoch == "" {
		c.IDGen.Epoch = "2026-06-21T00:00:00Z"
	}
	if c.IDGen.MaxClockBackwardMs == 0 {
		c.IDGen.MaxClockBackwardMs = 2000
	}
}

func (c *Config) validate() error {
	epoch, err := time.Parse(time.RFC3339, c.IDGen.Epoch)
	if err != nil {
		return fmt.Errorf("invalid idgen.epoch %q: %w", c.IDGen.Epoch, err)
	}
	if epoch.After(time.Now()) {
		return fmt.Errorf("idgen.epoch %q is in the future", c.IDGen.Epoch)
	}
	c.IDGen.epochTime = epoch
	return nil
}

// applyEnvOverrides lets the operator override a few hot values without
// editing the yaml (handy for containers / 12-factor deployments).
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("DB3_SERVER_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.Server.Port = p
		}
	}
	if v := os.Getenv("DB3_MYSQL_DSN"); v != "" {
		c.MySQL.DSN = v
	}
	if v := os.Getenv("DB3_MYSQL_HOST"); v != "" {
		c.MySQL.Host = v
	}
	if v := os.Getenv("DB3_MYSQL_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.MySQL.Port = p
		}
	}
	if v := os.Getenv("DB3_MYSQL_USER"); v != "" {
		c.MySQL.User = v
	}
	if v := os.Getenv("DB3_MYSQL_PASSWORD"); v != "" {
		c.MySQL.Password = v
	}
	if v := os.Getenv("DB3_MYSQL_DBNAME"); v != "" {
		c.MySQL.DBName = v
	}
	if v := os.Getenv("GIN_MODE"); v != "" {
		c.Server.Mode = v
	}
}
