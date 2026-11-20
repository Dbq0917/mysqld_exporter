// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blang/semver/v4"

	"github.com/prometheus/mysqld_exporter/auth"
)

const (
	FlavorMySQL   = "mysql"
	FlavorMariaDB = "mariadb"
)

// instanceConfig is owned by a single instance. Keeping the complete DSN and
// pool settings here ensures reconnects cannot borrow another target's state.
type instanceConfig struct {
	dsn             string
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	provider        auth.Provider
}

type instance struct {
	config            instanceConfig
	db                *sql.DB
	flavor            string
	version           semver.Version
	versionMajorMinor float64
}

func newInstance(ctx context.Context, dsn string, maxOpenConns, maxIdleConns int, connMaxLifetime time.Duration, provider auth.Provider) (*instance, error) {
	i := &instance{
		config: instanceConfig{
			dsn:             dsn,
			maxOpenConns:    maxOpenConns,
			maxIdleConns:    maxIdleConns,
			connMaxLifetime: connMaxLifetime,
			provider:        provider,
		},
	}
	if err := i.connect(ctx); err != nil {
		return nil, err
	}
	return i, nil
}

// connect builds a private pool from this instance's own connection settings.
func (i *instance) connect(ctx context.Context) error {
	connector, err := auth.OpenConnector(i.config.dsn, i.config.provider)
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(i.config.maxOpenConns)
	db.SetMaxIdleConns(i.config.maxIdleConns)
	db.SetConnMaxLifetime(i.config.connMaxLifetime)

	version, versionString, err := queryVersion(ctx, db)
	if err != nil {
		db.Close()
		return err
	}

	versionMajorMinor, err := strconv.ParseFloat(fmt.Sprintf("%d.%d", version.Major, version.Minor), 64)
	if err != nil {
		db.Close()
		return err
	}

	flavor := FlavorMySQL
	if strings.Contains(strings.ToLower(versionString), "mariadb") {
		flavor = FlavorMariaDB
	}

	i.db = db
	i.version = version
	i.versionMajorMinor = versionMajorMinor
	i.flavor = flavor
	return nil
}

// reconnect replaces only this instance's pool. The instance's immutable
// connection settings are reused, so another target's DSN or session parameters
// can never be picked up.
func (i *instance) reconnect(ctx context.Context) error {
	if i.db != nil {
		_ = i.db.Close()
		i.db = nil
	}
	return i.connect(ctx)
}

func (i *instance) getDB() *sql.DB {
	return i.db
}

func (i *instance) Close() error {
	if i.db == nil {
		return nil
	}
	defer func() {
		i.db = nil
	}()
	return i.db.Close()
}

// Ping checks connection availability and rebuilds this instance's pool if the
// existing connection cannot be used.
func (i *instance) Ping(ctx context.Context) error {
	if i.db == nil {
		return i.connect(ctx)
	}
	if err := i.db.PingContext(ctx); err != nil {
		if rerr := i.reconnect(ctx); rerr != nil {
			return err
		}
		return i.db.PingContext(ctx)
	}
	return nil
}

// The result of SELECT version() is something like:
// for MariaDB: "10.5.17-MariaDB-1:10.5.17+maria~ubu2004-log"
// for MySQL: "8.0.36-28.1"
var versionRegex = regexp.MustCompile(`^((\d+)(\.\d+)(\.\d+))`)

func queryVersion(ctx context.Context, db *sql.DB) (semver.Version, string, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT @@version;").Scan(&version)
	if err != nil {
		return semver.Version{}, version, err
	}

	matches := versionRegex.FindStringSubmatch(version)
	if len(matches) > 1 {
		parsedVersion, err := semver.ParseTolerant(matches[1])
		if err != nil {
			return semver.Version{}, version, fmt.Errorf("could not parse version from %q", matches[1])
		}
		return parsedVersion, version, nil
	}

	return semver.Version{}, version, fmt.Errorf("could not parse version from %q", version)
}
